package DBMS.fileManager.dataAcessManager.file.log;

import java.io.File;
import java.util.SortedMap;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import DBMS.Kernel;



public class HybridTreeLog implements LogHandle{

	private int LAST_LSN = -1;
	private static final int LAST_LSN_KEY = 1;
	
	private SortedMap<String, Long> map;
	private SortedMap<Integer, Integer> meta;
	private DB db;
	
	private SequentialLog sequentialLog;
	
	public HybridTreeLog(String file) {
		sequentialLog = new SequentialLog(file);
		
		db = DBMaker.openFile(Kernel.DATABASE_FILES_FOLDER+ File.separator + "hybrid_tree_log")
				.disableTransactions()
				.closeOnExit()
				.useRandomAccessFile()
				//.enableEncryption("password", false)
				.make();
		
		map = db.getTreeMap("log");
		if(map == null)map = db.createTreeMap("log");
		
		meta = db.getTreeMap("meta");
		if(meta == null)meta = db.createTreeMap("meta");
		
		Integer lastLsn = meta.get(LAST_LSN_KEY);
		if(lastLsn == null) {
			LAST_LSN = 0;
			meta.put(LAST_LSN_KEY, LAST_LSN);
		}else {
			LAST_LSN = lastLsn;
		}
		
		db.commit();
	}
	
	public synchronized void append(int lsn, int trasaction, char operation, String tupleID, String obj) {
		
		if(tupleID == null) return;
		
	
		long pointer = sequentialLog.getPointer();
		sequentialLog.append(lsn, trasaction, operation, tupleID, obj);
			

		map.put(tupleID, pointer);
		
		meta.put(LAST_LSN_KEY, lsn);
		db.commit();
	}

	public int readLastLSN() {
	
		return LAST_LSN;
	}

	public void interator(LogInterator interator) {
		interator(interator, true);
		
	}

	public void interator(LogInterator interator, boolean end) {

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
