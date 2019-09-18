package DBMS.fileManager.dataAcessManager.file.log;



public class IndexedSychronousLog extends IndexedAsychronousLog{

	public IndexedSychronousLog(String file) {
		super(file);
	}

	public synchronized void append(int lsn, int trasaction, char operation, String tupleID, String obj) {

		super.append(lsn, trasaction, operation, tupleID, obj);
		
		super.flush();
		
		
	}
	
	public void startSyncThread() {
		//nothing
	}

}
