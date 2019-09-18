package DBMS.fileManager.dataAcessManager.file.log;

import java.io.File;
import java.io.IOException;
import java.util.SortedMap;
import java.util.logging.Level;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import DBMS.Kernel;
import DBMS.recoveryManager.RedoLog;


//CHECK SYNC TREE!!!!
public class IndexedDoubleAsychronousLog implements LogHandle{

	protected long LAST_LSN = -1;
	protected long LAST_LSN_POINTER = -1;
	
	protected static final int LAST_LSN_KEY = 1;
	protected static final int LAST_LSN_POINTER_KEY = 2;
	

	protected long LAST_LSN_TREE_POINTER_CURRENT = -1;
	protected long LAST_LSN_LOG_CURRENT = -1;
	
	
	protected SortedMap<String, Long> map;
	protected SortedMap<Integer, Long> meta;
	protected DB db;

	protected long fix_Last_LSN_SEQ = -1;
	
	protected SequentialLog writeSequentialLog;
	
	protected SequentialLog readSequentialLog;
	
	protected String file;
	
	public IndexedDoubleAsychronousLog(String file) {
		this.file = file;
		writeSequentialLog = new SequentialLog(file);
		readSequentialLog = new SequentialLog(file);
		
		
		db = DBMaker.openFile(Kernel.DATABASE_FILES_FOLDER+ File.separator + this.getClass().getSimpleName())
				.disableTransactions()
				.closeOnExit()
				.useRandomAccessFile()
				//.enableEncryption("password", false)
				.make();
		
		map = db.getTreeMap("log");
		if(map == null)map = db.createTreeMap("log");
	
		
		meta = db.getTreeMap("meta");
		if(meta == null)meta = db.createTreeMap("meta");
		
		Long lastLsn = meta.get(LAST_LSN_KEY);
		if(lastLsn == null) {
			LAST_LSN = 0;
			meta.put(LAST_LSN_KEY, LAST_LSN);
		}else {
			LAST_LSN = lastLsn;
		}
		
		Long lastPointerLsn = meta.get(LAST_LSN_POINTER_KEY);
		if(lastPointerLsn == null) {
			LAST_LSN_POINTER = 0;
			meta.put(LAST_LSN_POINTER_KEY, LAST_LSN_POINTER);
		}else {
			LAST_LSN_POINTER = lastPointerLsn;
			LAST_LSN_TREE_POINTER_CURRENT = lastPointerLsn;
		}
		
		
		db.commit();
	}

	
	public void startSyncThread() {
		Kernel.log(IndexedDoubleAsychronousLog.class, "Synchronized Tree Thread Started", Level.CONFIG);
		new Thread(new Runnable() {
			

			@Override
			public void run() {

				try {
					while (Kernel.ENABLE_RECOVERY) {
						
						
						Thread.sleep(30000);
						syncSequentialToIndexed();
						db.commit();							
						

					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}).start();
	}
	
	public String getDataTuple(String tupleId) throws IOException {
		
		Long pointer = map.get(tupleId);
			
		
	//	SequentialLog readSequentialLog = new SequentialLog(file);
		
		String record = readSequentialLog.readRecord(pointer);

		String values[] = record.split(LOG_SEPARATOR);
//		int lsn = Integer.parseInt(values[0]);
//		int trasaction = Integer.parseInt(values[1]);
//		char operation = values[2].charAt(0);
//		long filePointer = -1;
		
		String obj = values[3];
		
		String stringTuple = obj.split(RedoLog.TUPLE_ID_SEPARATOR)[1];
		
		return stringTuple; 
	}
	
	
	public synchronized void append(int lsn, int trasaction, char operation, String tupleID, String obj) {

		writeSequentialLog.append(lsn, trasaction, operation, tupleID, obj);
		LAST_LSN_LOG_CURRENT = lsn;
		
		//flush();
		//writeSequentialLog.flush();
		
	}
	

	protected void appendTree(int lsn, String tupleID, long pointer) {
		map.put(tupleID, pointer);
		meta.put(LAST_LSN_KEY, (long)lsn);
		meta.put(LAST_LSN_POINTER_KEY, pointer);
		LAST_LSN_TREE_POINTER_CURRENT = pointer;
	}
	
	
	protected int countRecords = 0;
	public synchronized int syncSequentialToIndexed(){
		final Long target = LAST_LSN_LOG_CURRENT;
		readSequentialLog.interator(LAST_LSN_TREE_POINTER_CURRENT, new LogInterator() {
			
			boolean first = true;
			@Override
			public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {
				
				
				if(obj == null)return LogInterator.STOP;
				if(LAST_LSN_TREE_POINTER_CURRENT > 0 && first){
					first = false;
					return LogInterator.NEXT;
				}
				
				if(lsn >= target){
					return LogInterator.STOP;
				}
				
				
				String id = obj.split(RedoLog.TUPLE_ID_SEPARATOR)[0];
				
				appendTree(lsn, id, filePointer);
				countRecords++;
				return LogInterator.NEXT;
			}
			
			@Override
			public void error(Exception e) {
				e.printStackTrace();
				
			}
		}, false);
		int aux = countRecords;
		System.out.println("Sync Tree -- Records: " + aux + " LAST_LSN: " + target);
		countRecords = 0;
		return aux;
	}
	

	public int readLastLSN() {
		LAST_LSN_LOG_CURRENT = writeSequentialLog.readLastLSN();
		fix_Last_LSN_SEQ = LAST_LSN_LOG_CURRENT;
		return (int)LAST_LSN_LOG_CURRENT;
	}

	public void interator(LogInterator interator) {
		interator(interator, true);
		
	}

	public void interator(LogInterator interator, boolean end) {

		@SuppressWarnings("unused")
		char action = end ? LogInterator.PREV : LogInterator.NEXT;
		try {
			
			
			for (Long pointer : map.values()) {
			
					
				String record = readSequentialLog.readRecord(pointer);
		
				String values[] = record.split(LOG_SEPARATOR);
				int lsn = Integer.parseInt(values[0]);
				
				if(lsn <= fix_Last_LSN_SEQ ) {
					int trasaction = Integer.parseInt(values[1]);
					char operation = values[2].charAt(0);
					long filePointer = pointer;
					action = interator.readRecord(lsn, trasaction, operation, values[3], filePointer);	
				}else {
					System.out.println("PASS: " + fix_Last_LSN_SEQ + " >=" + lsn);
				}
				
				

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	public synchronized void syncTree() {
		
		long lStartTime = System.nanoTime();

		long lastSequetialLSN = readSequentialLog.readLastLSN();
		long lastTreeLSN = meta.get(LAST_LSN_KEY);

		if (lastTreeLSN < lastSequetialLSN) {
			Kernel.log(IndexedDoubleAsychronousLog.class, "Tree Log LSN Unsynchronized", Level.CONFIG);
			Kernel.log(IndexedDoubleAsychronousLog.class,"Last Tree LSN: " + lastTreeLSN + ", Last Sequential LSN: " + lastSequetialLSN, Level.CONFIG);

			readSequentialLog.interator(new LogInterator() {

				@Override
				public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {

					if (obj == null)
						return LogInterator.STOP;

					String id = obj.split(RedoLog.TUPLE_ID_SEPARATOR)[0];
					appendTree(lsn, id, filePointer);

					return LogInterator.NEXT;
				}

				@Override
				public void error(Exception e) {
					// TODO Auto-generated method stub

				}
			}, false);
			
			long newLastTreeLSN = meta.get(LAST_LSN_KEY); //NEW
			Kernel.log(this.getClass(),"Finish Synchronize Tree process... current LSN: "+newLastTreeLSN+"| total time: " + (System.nanoTime() - lStartTime) / 1000000 + " ms",Level.WARNING);
			
			db.commit();
			
		}
	}

	@Override
	public void flush() {
		db.commit();
		writeSequentialLog.flush();
		
	}


	@Override
	public void close() {
		db.close();
		writeSequentialLog.close();
		
		
	}
		

}
