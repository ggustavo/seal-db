package DBMS.transactionManager;

public interface TransactionManagerListener {
	
	
	void newTransaction(ITransaction transaction);
	void newTransactionOperation(TransactionOperation transactionOperation);
	void newTransactionOperationScheduled(TransactionOperation transactionOperation);
	
	void transactionCommit(ITransaction transaction);
	void transactionAbort(ITransaction transaction);
	void transactionFailed(ITransaction transaction);
	
	void newGraphEdgeConflit(ITransaction t1, ITransaction t2);
	
	
	void newLock(Lock lock);
	void unLock(Lock lock);
	void updateLock(Lock lock);
	
}
