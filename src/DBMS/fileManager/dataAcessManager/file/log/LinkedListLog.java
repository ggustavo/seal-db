package DBMS.fileManager.dataAcessManager.file.log;

import java.io.File;
import java.util.List;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

import DBMS.Kernel;



public class LinkedListLog implements LogHandle{


	
	private List<String> list;
	private DB db;
	
	public LinkedListLog(String file) {
	
		db = DBMaker.openFile(Kernel.DATABASE_FILES_FOLDER+ File.separator + "list_log")
				.disableTransactions()
				.closeOnExit()
				.useRandomAccessFile()
				//.enableEncryption("password", false)
				.make();
		
		list = db.getLinkedList("log");
		if(list == null)list = db.createLinkedList("log");

		db.commit();
	}
	
	public synchronized void append(int lsn, int trasaction, char operation, String tupleID, String obj) {
		
		if(tupleID == null) return;
		String record =  lsn + LOG_SEPARATOR
				   + trasaction  + LOG_SEPARATOR
				   + operation  + LOG_SEPARATOR
				   + obj;
		
		list.add(record);
		
		db.commit();
	}

	public int readLastLSN() {
	
		String record = list.get(list.size());
		String values[] = record.split(LOG_SEPARATOR);
		int lsn = Integer.parseInt(values[0]); 
		
		return lsn;
	}

	public void interator(LogInterator interator) {
		interator(interator, true);
		
	}

	public void interator(LogInterator interator, boolean end) {
		
		@SuppressWarnings("unused")
		char action = end ? LogInterator.PREV : LogInterator.NEXT;
		
		for (String record  : list) {
			//System.out.println("(l) -> " + l.toString());

			String values[] = record.split(LOG_SEPARATOR);
			int lsn = Integer.parseInt(values[0]);
			int trasaction = Integer.parseInt(values[1]);
			char operation = values[2].charAt(0);
			long filePointer = -1;

			action = interator.readRecord(lsn, trasaction, operation, values[3], filePointer);
			
		}
		
	}

}
