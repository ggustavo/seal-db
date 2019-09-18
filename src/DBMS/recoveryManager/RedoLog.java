package DBMS.recoveryManager;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Schema;
import DBMS.fileManager.dataAcessManager.file.log.FullTreeLog;
import DBMS.fileManager.dataAcessManager.file.log.IndexedAsychronousLog;
import DBMS.fileManager.dataAcessManager.file.log.IndexedDoubleAsychronousLog;
import DBMS.fileManager.dataAcessManager.file.log.IndexedDoubleAsychronousRecordTreeLog;
import DBMS.fileManager.dataAcessManager.file.log.IndexedSychronousLog;
import DBMS.fileManager.dataAcessManager.file.log.LogHandle;
import DBMS.fileManager.dataAcessManager.file.log.LogInterator;
import DBMS.fileManager.dataAcessManager.file.log.SequentialLog;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.TransactionOperation;

public class RedoLog implements IRecoveryManager {

	private LogHandle logHandle;
	private int CURRENT_LSN = 0;
	public static final String TUPLE_ID_SEPARATOR = "Æ";
	private boolean debug = false;
	
	
	public synchronized void startHandle(String file) {
		if(Kernel.LOG_STRATEGY == Kernel.SEQUENTIAL_lOG) {
			logHandle = new SequentialLog(file);
		}
		if(Kernel.LOG_STRATEGY == Kernel.FULL_TREE_lOG) {
			logHandle = new FullTreeLog(file);
		}
		if(Kernel.LOG_STRATEGY == Kernel.ASYCHRONOUS_INDEXED_LOG) {
			logHandle = new IndexedAsychronousLog(file);
		}
		if(Kernel.LOG_STRATEGY == Kernel.SYCHRONOUS_INDEXED_LOG) {
			logHandle = new IndexedSychronousLog(file);
		}
		
		if(Kernel.LOG_STRATEGY == Kernel.ASYCHRONOUS_DOUBLE_INDEXED_LOG) {
			logHandle = new IndexedDoubleAsychronousLog(file);
		}
		
		if(Kernel.LOG_STRATEGY == Kernel.ASYCHRONOUS_DOUBLE_INDEXED_RECORD_TREE_LOG) {
			logHandle = new IndexedDoubleAsychronousRecordTreeLog(file);
		}
		
		
		CURRENT_LSN = logHandle.readLastLSN();
	}
	
	


	public void start(String file) {
		if(!Kernel.ENABLE_RECOVERY)return;
		
		startHandle(file);
		
		if (Kernel.getFinalizeStateDatabase().equals(Kernel.DATABASE_FINALIZE_STATE_ERROR)) {
			if(Kernel.ENABLE_RECOVERY){
				Kernel.log(Kernel.class, "Use " + logHandle.getClass().getSimpleName() + " Strategy", Level.CONFIG);
			
				if(logHandle instanceof IndexedAsychronousLog ) {	
					((IndexedAsychronousLog) logHandle).syncTree();					
				}
				if(logHandle instanceof IndexedDoubleAsychronousRecordTreeLog){
					((IndexedDoubleAsychronousRecordTreeLog) logHandle).syncTree();		
				}
				if(logHandle instanceof IndexedDoubleAsychronousLog ) {	
					((IndexedDoubleAsychronousLog) logHandle).syncTree();					
				}
				
				
				if(Kernel.ENABLE_FAST_RECOVERY_STRATEGIE) {
					new Thread(new Runnable() {
						
						@Override
						public void run() {
							recovery();
							if(logHandle instanceof IndexedAsychronousLog) {
								((IndexedAsychronousLog) logHandle).startSyncThread();
							}
							if(logHandle instanceof IndexedDoubleAsychronousRecordTreeLog){
								((IndexedDoubleAsychronousRecordTreeLog) logHandle).startSyncThread();	
							}
							if(logHandle instanceof IndexedDoubleAsychronousLog ) {	
								((IndexedDoubleAsychronousLog) logHandle).startSyncThread();					
							}
							
							
						}
					}).start();
				}else {
					recovery();
					if(logHandle instanceof IndexedAsychronousLog) {
						((IndexedAsychronousLog) logHandle).startSyncThread();
					}
				}
		
			}
		} else {
			Kernel.log(this.getClass(),"Normal startup without recovery...",Level.WARNING);
		}

		Kernel.setFinalizeStateDatabase(Kernel.DATABASE_FINALIZE_STATE_ERROR);
		
	}

	public void safeFinalize() {
		if(Kernel.getScheduler()!=null)Kernel.getScheduler().abortAll();
		//if(Kernel.ENABLE_RECOVERY)addCheckPoint();
		//Kernel.setFinalizeStateDatabase(Kernel.DATABASE_FINALIZE_STATE_OK);
		logHandle.close();
	}
	

	
	private int cDelete = 0;
	private int cInserts= 0;
	private int cUpdates = 0;
	public void recovery() {
		Kernel.log(this.getClass(),"Start recovery process...",Level.WARNING);
		long lStartTime = System.nanoTime();
			
		Kernel.IN_RECOVERY_PROCESS = true;
		logHandle.interator(new LogInterator() {
			
			@Override
			public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {
				
				
				if(debug) {
					System.out.println("---------------------------------------");
					System.out.println("LSN: "+lsn);
					System.out.println("Transaction: T" +trasaction);
					System.out.println("operation: " +operation);
					System.out.println("log file pointer: "+filePointer);
					System.out.println("object: " +obj);					
				}
				//if(lsn >= CURRENT_LSN)return LogInterator.STOP;
				
				recoveryRecord(true,operation, obj);

				if(operation == TransactionOperation.BEGIN_TRANSACTION) {

				}
				if(operation == TransactionOperation.COMMIT_TRANSACTION) {
					
				}
				
				if(obj == null)return LogInterator.STOP;
				return LogInterator.NEXT;
			}
			
			@Override
			public void error(Exception e) {
				e.printStackTrace();
				
			}
		},false);
		
	
		RedoLog.EVENTS.add("R," + "-1" + "," + lStartTime + "," + System.nanoTime() );
		
		Kernel.log(this.getClass(),"Finish recovery process... total time: " + (System.nanoTime() - lStartTime) / 1000000 + " ms",Level.WARNING);

		//Kernel.log(this.getClass(),"Redo " + cTransactions + " Transactions",Level.WARNING);
		Kernel.log(this.getClass(),"Redo " + cInserts + " Inserts Records",Level.WARNING);
		Kernel.log(this.getClass(),"Redo " + cUpdates + " Updates Records",Level.WARNING);
		Kernel.log(this.getClass(),"Redo " + cDelete + " Deletes Records",Level.WARNING);
		Kernel.IN_RECOVERY_PROCESS = false;

	
	}
	
	public void undoTransaction(Transaction transaction) {
		if(!Kernel.ENABLE_RECOVERY)return;
		for (int i = transaction.getOperations().size()-1; i >= 0; i--) {
			TransactionOperation operation = transaction.getOperations().get(i);
			if(operation.getTuple() != null && operation.getTuple().getTable().isTemp()) {
				if(debug)System.out.println("-- TEMP\t\t"+operation);				
			}else {
				if(debug)System.out.println(operation);

				if(operation.getType() == TransactionOperation.WRITE_TUPLE) {
					
					Tuple tuple = operation.getTuple();
					MTable table = tuple.getTable();
					
					if(operation.getTupleOperation() == MTable.DELETE) {
						
						table.getTuplesHash().put(tuple.getTupleID(), tuple);
								
					}else
					if (operation.getTupleOperation() == MTable.UPDATE) {
						tuple.setData(operation.getBeforedata());

					}else
					if (operation.getTupleOperation() == MTable.INSERT) {
						
						table.getTuplesHash().remove(tuple.getTupleID());
								
					}else {
						Kernel.log(this.getClass(),"LOG OPERATION ERROR: " + transaction.getIdT(),Level.SEVERE);
					}
					
					
				}else if(operation.getType() != TransactionOperation.READ_TUPLE){
					
				}else {
					//READ!
				}
			}
			
		}
		//Kernel.log(this.getClass(),"Finish Undo Transaction: " + transaction.getIdT() + " operations: " + transaction.getOperations().size(),Level.WARNING);
	}
	
	
	
	public void commitTransaction(Transaction transaction) {
		if(!Kernel.ENABLE_RECOVERY)return;
		int writes = 0;
		for (int i = 0; i < transaction.getOperations().size(); i++) {
			TransactionOperation operation = transaction.getOperations().get(i);

			if(operation.getTuple()!= null && operation.getTuple().getTable().isTemp()) {
				if(debug)System.out.println(operation+" ...TEMP....");				
			}else if( operation.getTuple() != null ){
				
				if(operation.getTuple().getTable().isSystemTable()) {
					if(debug)System.out.println(operation+" ...SYSTEM TABLE....");	
					continue;
				}
				if(debug)System.out.println(operation);
				
				if(operation.getType() == TransactionOperation.WRITE_TUPLE) {
					
					if(operation.getTupleOperation() == MTable.DELETE) {
						logHandle.append(getNewLSN(), transaction.getIdT(), operation.getTupleOperation(), getGlobalTupleID(operation.getTuple()), getGlobalTupleID(operation.getTuple()) + TUPLE_ID_SEPARATOR + operation.getBeforedata());
						writes++;
					}else
					if (operation.getTupleOperation() == MTable.UPDATE) {
						logHandle.append(getNewLSN(), transaction.getIdT(), operation.getTupleOperation(), getGlobalTupleID(operation.getTuple()), getGlobalTupleID(operation.getTuple()) + TUPLE_ID_SEPARATOR + operation.getTuple().getStringData());
						writes++;
					
					}else
					if (operation.getTupleOperation() == MTable.INSERT) {
						logHandle.append(getNewLSN(), transaction.getIdT(), operation.getTupleOperation(), getGlobalTupleID(operation.getTuple()), getGlobalTupleID(operation.getTuple()) + TUPLE_ID_SEPARATOR + operation.getTuple().getStringData());
						writes++;
					}else {
						Kernel.log(this.getClass(),"LOG OPERATION ERROR: " + transaction.getIdT(),Level.SEVERE);
					}
					
					
				}else if(operation.getType() != TransactionOperation.READ_TUPLE){
					logHandle.append(getNewLSN(), transaction.getIdT(),operation.getType(), null, "---");
					writes++;
				}else {
					//READ
				}
				
			}else {
				if(debug)System.out.println(operation +" ...NULL....");
			}
			
		}

		// " operations: " + transaction.getOperations().size()
		Kernel.log(this.getClass(),"Finish Commit Transaction: " + transaction.getIdT()  + " Writes: " + writes,Level.WARNING);
		
	}
	
	private int getNewLSN() {
		return ++CURRENT_LSN;
	}
	
	private String getGlobalTupleID(Tuple tuple) {
		if(tuple.getTable() != null 
				&& tuple.getTable().getSchemaManipulate() != null ) {
			
			return tuple.getTable().getSchemaManipulate().getId() + "-" + tuple.getTable().getTableID() + "-"+tuple.getTupleID();
		}
		
		return null;
	}
	
	@Override
	public void printLog() {
		
		logHandle.interator(new LogInterator() {
			
			@Override
			public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {
				
				System.out.println("---------------------------------------");
				System.out.println("LSN: "+lsn);
				System.out.println("Transaction: T" +trasaction);
				System.out.println("operation: " +operation);
				System.out.println("log file pointer: "+filePointer);
				System.out.println("object: " +obj);
				
				if(obj == null)return LogInterator.STOP;
				
				return LogInterator.PREV;
			}
			
			@Override
			public void error(Exception e) {
				e.printStackTrace();
				
			}
		});
		

		
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

	@Override
	public synchronized Tuple getRecord(MTable table, String tupleId) {
		
		try {
			String gId = table.getSchemaManipulate().getId() + "-" + table.getTableID() + "-" + tupleId;
			
			String tupleData = logHandle.getDataTuple(gId);
			
			
			//System.out.println("Recovery: " + tupleData);
			
			Tuple tuple = new Tuple(table, tupleId,  tupleData.split("\\"+MTable.SEPARATOR));
			tuple.isRecovered = true;
			table.getTuplesHash().put(tupleId, tuple);
			
			
			return tuple;
			
			
		} catch (Exception e) {
			//e.printStackTrace();
			//Kernel.exception(this.getClass(), e);
			return null;
		}
		
		
		//Tuple tuple = new Tuple(this, String.valueOf(id), lineToArray(tupleData));
		
		//		String stringTuple;
//		
//		String schemaID = id.split("-")[0];
//		String tableID = id.split("-")[1];
//		String tupleID = id.split("-")[2];
//	
//		Schema schema = Kernel.getCatalog().getSchemabyId(schemaID);
//		MTable table = schema.getTableById(tableID);
//		
		//Tuple tuple = new Tuple(table, tupleID, stringTuple.split("\\"+MTable.SEPARATOR));
//		table.getTuplesHash().put(tupleID, tuple);
//		table.setLastTupleWrited(Integer.parseInt(tupleID));
//		
	}
	

	public synchronized void recoveryRecord(boolean statics, char operation, String obj) {
		if (operation == MTable.DELETE) {
			if(statics)cDelete++;
			String id = obj.split(TUPLE_ID_SEPARATOR)[0];
		//	String stringTuple = obj.split(TUPLE_ID_SEPARATOR)[1];
			
			String schemaID = id.split("-")[0];
			String tableID = id.split("-")[1];
			String tupleID = id.split("-")[2];
				
			Schema schema = Kernel.getCatalog().getSchemabyId(schemaID);
			MTable table = schema.getTableById(tableID);

			table.getTuplesHash().remove(tupleID);
			
		} 
		
		if (operation == MTable.UPDATE) {
			if(statics)cUpdates++;
			String id = obj.split(TUPLE_ID_SEPARATOR)[0];
			String stringTuple = obj.split(TUPLE_ID_SEPARATOR)[1];
			
			String schemaID = id.split("-")[0];
			String tableID = id.split("-")[1];
			String tupleID = id.split("-")[2];
			
			Schema schema = Kernel.getCatalog().getSchemabyId(schemaID);
			MTable table = schema.getTableById(tableID);

			Tuple t = table.getTuplesHash().get(tupleID);
			if(t==null) {
				t= new Tuple(table, tupleID, stringTuple.split("\\"+MTable.SEPARATOR));
				t.isRecovered = true;
				table.getTuplesHash().put(tupleID, t);
			}else {
				t.setData(stringTuple.split("\\"+MTable.SEPARATOR));
				t.isRecovered = true;
			}

			
		}
		
		if (operation == MTable.INSERT) {
			if(statics)cInserts++;
			String id = obj.split(TUPLE_ID_SEPARATOR)[0];
			String stringTuple = obj.split(TUPLE_ID_SEPARATOR)[1];
			
			String schemaID = id.split("-")[0];
			String tableID = id.split("-")[1];
			String tupleID = id.split("-")[2];
				
			Schema schema = Kernel.getCatalog().getSchemabyId(schemaID);
			MTable table = schema.getTableById(tableID);

			Tuple tuple = new Tuple(table, 	tupleID, stringTuple.split("\\"+MTable.SEPARATOR));
			tuple.isRecovered = true;
			table.getTuplesHash().put(tupleID, tuple);
				
		}
	}
	
	
	public void forceFlush() {
		logHandle.flush();
	}
	
	public void forceClose() {
		logHandle.close();
	}
	
	
	public static LinkedList<String > EVENTS = new LinkedList<String>();

	
	public static void saveLogEvents() {
//		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
//		String date = dt.format(new Date());
		
		try {
			String instat = Kernel.ENABLE_FAST_RECOVERY_STRATEGIE ? "instant" : "no_instant";
			String name = Kernel.getRecoveryManager().getLogHandle().getClass().getSimpleName()+"_"+instat+".csv";
			PrintWriter logRequests = new PrintWriter(new FileWriter(new File(name) ,  true));
	
			for (String string : EVENTS) {
				logRequests.println(string);				
			}
				
			logRequests.flush();
			logRequests.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public LogHandle getLogHandle() {
		return logHandle;
	}

	
}
