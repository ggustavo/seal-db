package DBMS.recoveryManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.fileManager.dataAcessManager.DataAcess;
import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
import DBMS.fileManager.dataAcessManager.file.data.FileTuple;
import DBMS.fileManager.dataAcessManager.file.log.FileLog;
import DBMS.fileManager.dataAcessManager.file.log.FileRecord;
import DBMS.fileManager.dataAcessManager.file.log.FileTempLog;
import DBMS.fileManager.dataAcessManager.file.log.FileLog.LogPointer;
import DBMS.queryProcessing.TupleManipulate;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionOperation;

public class RecoveryManager implements IRecoveryManager {

	private FileLog log;

	private RecoveryManagerListener recoveryManagerListen;
	
	private static final boolean LOG_DEBUG = true;

	private int countCheckpoint = 0;
	private String path;
	
	public void start(String path) {
		this.path = path;
		log = new FileLog(path);

		if (Kernel.getFinalizeStateDatabase().equals(Kernel.DATABASE_FINALIZE_STATE_ERROR)) {
			if(Kernel.ENABLE_RECOVERY){
				if(LOG_DEBUG)Kernel.log(this.getClass(),"Start recovery process...",Level.WARNING);
				recovery();				
			}
		} else {
			if(LOG_DEBUG)Kernel.log(this.getClass(),"Normal startup without recovery...",Level.WARNING);
		}

		Kernel.setFinalizeStateDatabase(Kernel.DATABASE_FINALIZE_STATE_ERROR);
		if(Kernel.ENABLE_RECOVERY)addCheckPoint();
		
	}
	
	public FileLog getFileLogAcess(){
		return new FileLog(path);
	}

	private void addCheckPoint() {
		FileRecord f = new FileRecord(FileRecord.CHECKPOINT_RECORD_TYPE);
		f.setLSN(getNewLSN());
		f.setDate(new Date());
		Kernel.getBufferManager().flush();
		log.append(f);
		if(recoveryManagerListen!=null)recoveryManagerListen.newRecord(f);
		//if(LOG_DEBUG)showRecord(f);
	}

	private int getNewLSN() {
		LogPointer pointer = log.getPointer();
		FileRecord f = log.readPrev(pointer);

		if (f != null) {
			int last = f.getLSN();
			return last + 1;
		}
		return 0;
	}
	
	static class LogTransactionController{
		Integer id;
		boolean isBegin = false;
		boolean redo = false;
		public LogTransactionController(Integer id) {
			this.id = id;
		}
		@Override
		public String toString() {
			return "T" + id;
		}
		
	}
	
	
	private boolean contains(List<LogTransactionController> list, int id){
		for (LogTransactionController ltc : list) {
			if(ltc.id == id)return true;
		}
		return false;
	}
	
	
	public void printLog(){
		
		if(LOG_DEBUG)Kernel.log(this.getClass(),"Show log file",Level.INFO);
		LogPointer pointer = log.getPointer();
		FileRecord f = log.readPrev(pointer);
		while(f!=null){
			showRecord(f);
			f = log.readPrev(pointer);
		}
		if(LOG_DEBUG)Kernel.log(this.getClass(),"Finish log file",Level.INFO);
	}

	

	
	
	private void recovery() {
		//printLog();
		if(LOG_DEBUG)Kernel.log(this.getClass(),"Start analyze process...",Level.WARNING);
		
		
		FileTempLog tempLog = new FileTempLog(Kernel.getCatalog().getDefaultSchema().getTempFolder() + File.separator + "temp_log" + hashCode() + ".b");
		
		List<LogTransactionController> committed = new LinkedList<>();	
		List<LogTransactionController> aborted = new LinkedList<>();	
		List<LogTransactionController> notCompleted = new LinkedList<>();	
		HashMap<Integer, LogTransactionController> map = new HashMap<>();
		
		LogPointer pointer = log.getPointer();
		int type;
		FileRecord f = log.readPrev(pointer);
		
	
		if(f == null || f.getRecordType() == FileRecord.CHECKPOINT_RECORD_TYPE){
			if(LOG_DEBUG)Kernel.log(this.getClass(),"Finish analyze process...",Level.WARNING);
			if(LOG_DEBUG)Kernel.log(this.getClass(),"Recovery done!",Level.WARNING);
			return;
		}
		
		while (f != null) {
			//if(LOG_DEBUG)showRecord(f);
			type = f.getRecordType();
			
			if(type == FileRecord.CHECKPOINT_RECORD_TYPE){
				
				List<LogTransactionController> notFoundBegin = new LinkedList<>();
				for (LogTransactionController ltc : map.values()) {
					if(!ltc.isBegin){
						notFoundBegin.add(ltc);
					}
				}
				
				if(!notFoundBegin.isEmpty()){
					if(LOG_DEBUG)Kernel.log(this.getClass(),"There are pre-checkpoint records that need to be Treated",Level.WARNING);
					
					while (f != null && !notFoundBegin.isEmpty()) {
						type = f.getRecordType();
						
						if(contains(notFoundBegin, f.getTransactionId())){
							
							if(type == FileRecord.UPDATE_LOG_RECORD_TYPE ){
								analyzeUpdateLogRecord(tempLog, notCompleted, map, f);
							}
							
							if(type == FileRecord.BENGIN_RECORD_TYPE){
								LogTransactionController found = analyzeBeginRecord(map, f);
								if(found != null)notFoundBegin.remove(found);
							}	
						}
						f = log.readPrev(pointer);
					}
					
					if(!notFoundBegin.isEmpty()){
						if(LOG_DEBUG)Kernel.log(this.getClass(),"There are missing records",Level.SEVERE);
					}
				}
				
				break;
			}
		
			if(type == FileRecord.COMMIT_RECORD_TYPE){
				LogTransactionController l = new LogTransactionController(f.getTransactionId());
				l.redo = true;
				map.put(l.id, l);
				committed.add(l);
			}
			
			if(type == FileRecord.ABORT_RECORD_TYPE){
				LogTransactionController l = new LogTransactionController(f.getTransactionId());
				map.put(l.id, l);
				l.redo = false;
				aborted.add(l);
			}
			
			
			if(type == FileRecord.UPDATE_LOG_RECORD_TYPE){
				analyzeUpdateLogRecord(tempLog, notCompleted, map, f);
			}
			
			if(type == FileRecord.BENGIN_RECORD_TYPE){
				analyzeBeginRecord(map, f);
			}
			
			f = log.readPrev(pointer);
		}
		
		if(LOG_DEBUG)Kernel.log(this.getClass(),"Finish analyze process...",Level.WARNING);
		
		if(LOG_DEBUG)showList(committed, "Committed Transactions:");
		if(LOG_DEBUG)showList(aborted, "Aborted Transactions:");
		if(LOG_DEBUG)showList(notCompleted, "Not completed Transactions:");
		
		if(LOG_DEBUG)Kernel.log(this.getClass(),"Start Redo and Undo Process...",Level.WARNING);
		
		//UNDO

		for (int i = 0; i < tempLog.getSize(); i++) {

			FileRecord r = tempLog.readIndex(i);

			LogTransactionController ltc = map.get(r.getTransactionId());

			if (ltc != null) {
				if (!ltc.redo) {
					if(LOG_DEBUG)Kernel.log(this.getClass(),"Try UNDO   >> LSN: " + r.getLSN() + " from transaction:" + r.getTransactionId(),Level.INFO);
					undo(r);
				}
			} else {
				if(LOG_DEBUG)Kernel.log(this.getClass(),"record not found: " + r.getLSN(),Level.SEVERE);
			}

		}

		// REDO
		
		for (int i = tempLog.getSize() - 1; i >=  0; i--) {

			FileRecord r = tempLog.readIndex(i);

			LogTransactionController ltc = map.get(r.getTransactionId());

			if (ltc != null) {

				if (ltc.redo) {
					if(LOG_DEBUG)Kernel.log(this.getClass(),"Try REDO   >> LSN: " + r.getLSN() + " from transaction:" + r.getTransactionId(),Level.INFO);
					redo(r);
				}

			} else {
				if(LOG_DEBUG)Kernel.log(this.getClass(),"record not found: " + r.getLSN(),Level.SEVERE);
			}
		}

		if(LOG_DEBUG)Kernel.log(this.getClass(),"Recovery done!",Level.WARNING);
		
	}

	private LogTransactionController analyzeBeginRecord(HashMap<Integer, LogTransactionController> map, FileRecord f) {
		LogTransactionController ltc = map.get(f.getTransactionId());
		if(ltc!=null){
			ltc.isBegin = true;
			return ltc;
		}
		return null;
	}

	private void analyzeUpdateLogRecord(FileTempLog tempLog, List<LogTransactionController> notCompleted,
			HashMap<Integer, LogTransactionController> map, FileRecord f) {
		LogTransactionController ltc = map.get(f.getTransactionId());
		
		if(ltc == null){
			LogTransactionController l = new LogTransactionController(f.getTransactionId());
			map.put(l.id, l);
			notCompleted.add(l);
			l.redo = false;
		}
		
		tempLog.append(f);
	}
	
	

	private void redo(FileRecord r){
		try{
			byte[] b = DataAcess.readBlock(r.getSchemaID()+"-"+r.getTableID()+"-"+r.getBlockID());
			int storageLSN = new FileBlock(b).getLSN();
			if(LOG_DEBUG)Kernel.log(this.getClass(),"Compare: Log LSN: "+r.getLSN()+" <-x-> Storage LSN: " + storageLSN + " " + (storageLSN < r.getLSN()),Level.WARNING);
			if(storageLSN < r.getLSN()) DataAcess.writeBlock(r.getSchemaID()+"-"+r.getTableID()+"-"+r.getBlockID(), r.getAfterImage());
		}catch (Exception e) {
			Kernel.exception(this.getClass(),e);
		}
	}

	private void undo(FileRecord r){
		try{
			byte[] b = DataAcess.readBlock(r.getSchemaID()+"-"+r.getTableID()+"-"+r.getBlockID());
			int storageLSN = new FileBlock(b).getLSN();
			if(LOG_DEBUG)Kernel.log(this.getClass(),"Compare: Log LSN: "+r.getLSN()+" <-x-> Storage LSN: " + storageLSN + " " + (storageLSN < r.getLSN()),Level.WARNING);
			if(storageLSN > r.getLSN()) DataAcess.writeBlock(r.getSchemaID()+"-"+r.getTableID()+"-"+r.getBlockID(), r.getBeforeImage());			
		}catch (Exception e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	
	
	public void safeFinalize() {
		Kernel.getScheduler().abortAll();
		if(Kernel.ENABLE_RECOVERY)addCheckPoint();
		Kernel.setFinalizeStateDatabase(Kernel.DATABASE_FINALIZE_STATE_OK);
	//	printLog();
	}
	
	
	


	
	public synchronized int appendOrUndoTransaction(TransactionOperation operation, ITransaction transaction, char action) {

		if(Kernel.ENABLE_RECOVERY){
	
			if(action == ACTION_APPEND_OPERATION){
				
				return appendRecord(operation); 
				
			}else if(action == ACTION_UNDO_TRASACTION ){
				
				undoTransaction(transaction);
				
			}else{
				if(LOG_DEBUG)Kernel.log(this.getClass(),"Invalid Action: "+action,Level.SEVERE);
			}
		}
		
		return -1;
	}

	private void undoTransaction(ITransaction transaction) {
		if(transaction != null){
			
			LogPointer pointer = log.getPointer();
			FileRecord f = log.readPrev(pointer);
			boolean finish = false;
			boolean find = false;
			
			while (f != null) {
				
				if(f.getTransactionId() == transaction.getIdT()){
					find = true;
					
		
					if(f.getRecordType() == FileRecord.BENGIN_RECORD_TYPE){
						finish = true;
						break;
					}
					
					if(f.getRecordType() == FileRecord.UPDATE_LOG_RECORD_TYPE){
						
						IPage page = Kernel.getBufferManager().getPage(transaction, f.getSchemaID() +"-"+ f.getTableID()+"-"+f.getBlockID() ,TransactionOperation.WRITE_TRANSACTION,false); //TODO tests here
						page.setData(f.getBeforeImage());
					}

				}
				
				f = log.readPrev(pointer);
			}
			
			
			if(find==false){
				if(LOG_DEBUG)Kernel.log(this.getClass(),"Undo not completed, transaction: "+transaction.getIdT()+" not found",Level.SEVERE);
			}
			
			if(finish){
				if(LOG_DEBUG)Kernel.log(this.getClass(),"Undo Transaction: " + transaction.getIdT() + " done!",Level.WARNING);
			}else{
				if(LOG_DEBUG)Kernel.log(this.getClass(),"Begin Transaction: " + transaction.getIdT() +" not found",Level.SEVERE);
			}
			
			
		}else{
			if(LOG_DEBUG)Kernel.log(this.getClass(),"Transaction null",Level.SEVERE);
		}
	}

	private int appendRecord(TransactionOperation operation) {
		if (operation != null) {

			FileRecord record = null;

			switch (operation.getType()) {
			case TransactionOperation.COMMIT_TRANSACTION:

				record = createRecord(operation, FileRecord.COMMIT_RECORD_TYPE);

				break;
			case TransactionOperation.ABORT_TRANSACTION:

				record = createRecord(operation, FileRecord.ABORT_RECORD_TYPE);

				break;
			case TransactionOperation.BEGIN_TRANSACTION:

				record = createRecord(operation, FileRecord.BENGIN_RECORD_TYPE);

				break;
			case TransactionOperation.WRITE_TRANSACTION:
				
				if(!canSaveOperation(operation)){
					//if(LOG_DEBUG)LogError.save(this.getClass(),"Dont save:" + operation.getObjectDatabaseId());
					return -1;
				}

				record = createRecord(operation, FileRecord.UPDATE_LOG_RECORD_TYPE);
				configIds(operation, record);
				record.setAfterImage(operation.getAfterImage());
				record.setBeforeImage(operation.getBeforeImage());
				break;
			default:
				if(LOG_DEBUG)Kernel.log(this.getClass(),"Invalid Operation Type: " + operation.getType() ,Level.SEVERE);
				break;
			}

			log.append(record);
			//if(LOG_DEBUG)showRecord(record);
			
			
			countCheckpoint++;
			if(countCheckpoint == Kernel.CHECKPOINT_INTERVAL){
				addCheckPoint();
				if(LOG_DEBUG)Kernel.log(this.getClass(),"New checkpoint added",Level.WARNING);
				countCheckpoint = 0;
			}
			if(recoveryManagerListen!=null)recoveryManagerListen.newRecord(record);
			return record.getLSN();
		}else{
			if(LOG_DEBUG)Kernel.log(this.getClass(),"Operation null",Level.SEVERE);
		}
		return -1;
	}
	
	private boolean canSaveOperation(TransactionOperation operation){
		
		FileBlock fileBlock = new FileBlock(operation.getAfterImage());

		if(fileBlock.isTemp()==1){
			return false;
		}
		
		return true;
	}

	private FileRecord createRecord(TransactionOperation operation, int type) {
		FileRecord f = new FileRecord(type);
		f.setDate(new Date());
		f.setLSN(getNewLSN());
		f.setTransactionId(operation.getTransaction().getIdT());
		return f;
	}

	private void configIds(TransactionOperation operation, FileRecord fileRecord) {
		ObjectDatabaseId o = operation.getObjectDatabaseId();
		fileRecord.setSchemaID(Integer.parseInt(o.getSchemaID()));
		fileRecord.setTableID(Integer.parseInt(o.getTableID()));
		fileRecord.setBlockID(Integer.parseInt(o.getBlockID()));
		if(o.getTupleID()!=null){
			fileRecord.setTupleID(Integer.parseInt(o.getTupleID()));			
		}
	}


	public static void showRecord(FileRecord r) {
		String s = "";
		s+="-----------------------------------"+"\n";
		s+="LSN: " + r.getLSN()+"\n";
		s+="Transaction: " + r.getTransactionId()+"\n";
		s+="Type: " + getTypeString(r.getRecordType())+"\n";
		s+="Date: " + r.getDate()+"\n";

		if (r.getRecordType() == FileRecord.UPDATE_LOG_RECORD_TYPE) {
			
			
			s+="Schema: " + r.getSchemaID()+"\n";
			s+="Table: " + r.getTableID()+"\n";
			s+="Block: " + r.getBlockID()+"\n";
			s+="Tuple: " + r.getTupleID()+"\n";
			
			s+="BeforeImage: "+"\n";
			FileBlock fileBlock = new FileBlock(r.getBeforeImage());
			if(fileBlock.getStatus()==-1)s+="-null-"+"\n";
			ArrayList<FileTuple> ftuples = fileBlock.readTuplesArray();
			for (FileTuple fileTuple : ftuples) {
				s+="    "+new TupleManipulate(fileTuple.getTupleID(),fileTuple.getData()).getStringData()+"\n";
			}
	
			s+="AfterImage: "+"\n";
			fileBlock = new FileBlock(r.getAfterImage());
			if(fileBlock.getStatus()==-1)s+="-null-"+"\n";
			ftuples = fileBlock.readTuplesArray();
			for (FileTuple fileTuple : ftuples) {
				s+="    "+new TupleManipulate(fileTuple.getTupleID(),fileTuple.getData()).getStringData()+"\n";
			}
	
	
		}
		s+="-----------------------------------"+"\n";
		Kernel.log(RecoveryManager.class,s,Level.INFO);
	}
	
	
	private static String getTypeString(int f) {

		switch (f) {
		case FileRecord.UPDATE_LOG_RECORD_TYPE:
			return "Update log";
		case FileRecord.COMMIT_RECORD_TYPE:
			return "Commit";
		case FileRecord.ABORT_RECORD_TYPE:
			return "Abort";
		case FileRecord.BENGIN_RECORD_TYPE:
			return "Begin";
		case FileRecord.CHECKPOINT_RECORD_TYPE:
			return "checkpoint";
		default:
			break;
		}
		return f + " [ERR0] Unknown type";
	}
	
	private void showList(List<?> list, String title){
		if(!list.isEmpty())Kernel.log(this.getClass(),title,Level.INFO);
		for (Object o : list) {
			Kernel.log(this.getClass(),"-" + o.toString(),Level.INFO);
		}
	}

	public RecoveryManagerListener getRecoveryManagerListen() {
		return recoveryManagerListen;
	}

	public void setRecoveryManagerListen(RecoveryManagerListener recoveryManagerListen) {
		this.recoveryManagerListen = recoveryManagerListen;
	}
	
	public List<FileRecord> findByDate(Date date1, Date date2){
		List<FileRecord> result = new LinkedList<>();
		LogPointer pointer = log.getPointer();
		FileRecord f = log.readPrev(pointer);
		while(f!=null){
			
			Date fDate = f.getDate();
			if(fDate.after(date1) && fDate.before(date2)){
				result.add(f);
			}
			
			f = log.readPrev(pointer);
		}
		return result;
	}
	
	public List<FileRecord> findByLSN(int LSN1, int LSN2){
		List<FileRecord> result = new LinkedList<>();
		LogPointer pointer = log.getPointer();
		FileRecord f = log.readPrev(pointer);
		while(f!=null){
			
			int FLSN = f.getLSN();
			if(FLSN >= LSN1 && FLSN <= LSN2){
				result.add(f);
			}
			
			f = log.readPrev(pointer);
		}
		return result;
	}
	
	public List<FileRecord> findByTransaction(int transaction){
		List<FileRecord> result = new LinkedList<>();
		LogPointer pointer = log.getPointer();
		FileRecord f = log.readPrev(pointer);
		while(f!=null){
			
			if(f.getTransactionId() == transaction && transaction != 0){
				result.add(f);
			}
			f = log.readPrev(pointer);
		}
		return result;
	}
	
	
}
