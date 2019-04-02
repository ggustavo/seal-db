package DBMS.transactionManager;

import DBMS.queryProcessing.queryEngine.AcquireLockException;

public interface TransactionRunnable{
	void run(Transaction transaction) throws AcquireLockException;
	void onFail(Transaction transaction, Exception e);
}
