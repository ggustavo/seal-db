package DBMS.fileManager.dataAcessManager.file.log;

import java.io.File;
import java.util.SortedMap;
import java.util.logging.Level;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import DBMS.Kernel;
import DBMS.recoveryManager.RedoLog;



public class ParallelHybridTreeLog implements LogHandle{

	private long LAST_LSN = -1;
	private long LAST_LSN_POINTER = -1;
	
	private static final int LAST_LSN_KEY = 1;
	private static final int LAST_LSN_POINTER_KEY = 2;
	
	private SortedMap<String, Long> map;
	private SortedMap<Integer, Long> meta;
	private DB db;
	public static int ITERVAL = 20; 
	
	
	private SequentialLog sequentialLog;
	
	public ParallelHybridTreeLog(String file) {
		sequentialLog = new SequentialLog(file);
		
		db = DBMaker.openFile(Kernel.DATABASE_FILES_FOLDER+ File.separator + "parallel_hybrid_tree_log")
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
		}
		
		
		db.commit();
	}

	
	public synchronized void append(int lsn, int trasaction, char operation, String tupleID, String obj) {
		append(lsn, trasaction, operation, tupleID, obj,-2);
	}
	
	private int count_append = 0;

	public synchronized void append(int lsn, int trasaction, char operation, String tupleID, String obj, long pointer) {
		
		if(tupleID == null) return;
		
		if(pointer == -2) {
			pointer = sequentialLog.getPointer();
			sequentialLog.append(lsn, trasaction, operation, tupleID, obj);
		}
		
		map.put(tupleID, pointer);
		
		meta.put(LAST_LSN_KEY, (long)lsn);
		meta.put(LAST_LSN_POINTER_KEY, pointer);
		
		if(pointer == -2) {
			count_append ++;
			if(count_append >= ITERVAL) {
				db.commit();
				count_append = 0;
			}
		}
		
	}

	public int readLastLSN() {
	
		return (int)LAST_LSN;
	}

	public void interator(LogInterator interator) {
		interator(interator, true);
		
	}

	public void interator(LogInterator interator, boolean end) {

		long lastSequetialLSN = sequentialLog.readLastLSN();
		long lastTreeLSN = this.readLastLSN();
		
		
		if(lastTreeLSN < lastSequetialLSN) {
			Kernel.log(ParallelHybridTreeLog.class,"Tree Log LSN Unsynchronized" , Level.CONFIG);
			System.out.println("Last Tree LSN: " + lastTreeLSN + ", Last Sequential LSN: " + lastSequetialLSN);
		
			
			sequentialLog.interator(LAST_LSN_POINTER, new LogInterator() {
				
				@Override
				public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {

					if(obj == null)return LogInterator.STOP;

					String id = obj.split(RedoLog.TUPLE_ID_SEPARATOR)[0];
					ParallelHybridTreeLog.this.append(lsn, trasaction, operation, id, obj, filePointer);
					

					return LogInterator.NEXT;
				}
				
				@Override
				public void error(Exception e) {
					e.printStackTrace();
					
				}
			}, false);
			db.commit();
		}
		
		@SuppressWarnings("unused")
		char action = end ? LogInterator.PREV : LogInterator.NEXT;
		try {

			for (Long pointer : map.values()) {
				

				String record = sequentialLog.readRecord(pointer);
		
				String values[] = record.split(LOG_SEPARATOR);
				int lsn = Integer.parseInt(values[0]);
				int trasaction = Integer.parseInt(values[1]);
				char operation = values[2].charAt(0);
				long filePointer = -1;

				action = interator.readRecord(lsn, trasaction, operation, values[3], filePointer);

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
		

}
