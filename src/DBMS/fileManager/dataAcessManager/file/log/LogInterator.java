package DBMS.fileManager.dataAcessManager.file.log;

public interface LogInterator {
	public static final char STOP = 'S';
	public static final char NEXT = 'N';
	public static final char PREV = 'P';
	
	char readRecord(int lsn, int trasaction,char operation,String obj, long filePointer);
	void error(Exception e);
}