package DBMS.recoveryManager;

import DBMS.fileManager.dataAcessManager.file.log.LogHandle;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.transactionManager.Transaction;

public interface IRecoveryManager {

	void start(String path);

	void printLog();

	void safeFinalize();
	
	void undoTransaction(Transaction transaction);
	
	void commitTransaction(Transaction transaction);
	
	Tuple getRecord(MTable m, String id);
	
	void forceFlush();
	
	void forceClose();
	
	void recoveryRecord(boolean statics, char operation, String obj);
	
	LogHandle getLogHandle();

}