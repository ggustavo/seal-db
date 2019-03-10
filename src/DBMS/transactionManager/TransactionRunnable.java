package DBMS.transactionManager;

public interface TransactionRunnable{
	void run(ITransaction transaction);
	void onFail(ITransaction transaction, Exception e);
}
