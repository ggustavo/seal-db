package DBMS.recoveryManager;

import DBMS.transactionManager.Transaction;

public interface IRecoveryManager {

	void start(String path);

	void printLog();

	void safeFinalize();
	
	void undoTransaction(Transaction transaction);
	
	void commitTransaction(Transaction transaction);

}