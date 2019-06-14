package DBMS.fileManager.dataAcessManager.file.log;

import java.io.IOException;

public interface LogHandle {

	public final static String LOG_SEPARATOR = "ϕ";
	
	void append(int lsn, int trasaction, char operation,String tupleID, String obj);

	int readLastLSN();

	void interator(LogInterator interator);

	void interator(LogInterator interator, boolean end);
	
	void flush();
	
	void close();
	
	String getDataTuple(String tupleId) throws IOException;

}