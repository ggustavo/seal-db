package DBMS.fileManager.dataAcessManager.file.log;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.SortedMap;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import DBMS.Kernel;
import DBMS.recoveryManager.RedoLog;



public class FullTreeLog implements LogHandle{

	private int LAST_LSN = -1;
	private static final int LAST_LSN_KEY = 1;
	
	private SortedMap<String, LinkedList<String>> map;
	private SortedMap<Integer, Integer> meta;
	private DB db;
	
	public FullTreeLog(String file) {
	
		db = DBMaker.openFile(Kernel.DATABASE_FILES_FOLDER+ File.separator + "tree_log")
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
		String record =  lsn + LOG_SEPARATOR
				   + trasaction  + LOG_SEPARATOR
				   + operation  + LOG_SEPARATOR
				   + obj;
		
		LinkedList<String> list = map.get(tupleID);
		if(list==null) {
			list = new LinkedList<>();
			
		}
		list.add(record);
		map.put(tupleID, list);
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
		
		for (LinkedList<String> l : map.values()) {
			//System.out.println("(l) -> " + l.toString());

			String record = l.getLast();

			String values[] = record.split(LOG_SEPARATOR);
			int lsn = Integer.parseInt(values[0]);
			int trasaction = Integer.parseInt(values[1]);
			char operation = values[2].charAt(0);
			long filePointer = -1;

			action = interator.readRecord(lsn, trasaction, operation, values[3], filePointer);
			
		}
		
	}
	
	
	public synchronized String getDataTuple(String tupleId) throws IOException {
		
		
		LinkedList<String> l = map.get(tupleId);
		
		String record = l.getLast();

		String values[] = record.split(LOG_SEPARATOR);
//		int lsn = Integer.parseInt(values[0]);
//		int trasaction = Integer.parseInt(values[1]);
//		char operation = values[2].charAt(0);
//		long filePointer = -1;
		
		String obj = values[3];
		
		String stringTuple = obj.split(RedoLog.TUPLE_ID_SEPARATOR)[1];
		
		return stringTuple; 
	}
	
	
	@Override
	public void flush() {
		db.commit();
		
		
	}


	@Override
	public void close() {
		db.close();
		
		
		
	}
	

}
