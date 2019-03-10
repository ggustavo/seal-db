package DBMS.queryProcessing;


import DBMS.connectionManager.DBConnection;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;

public class ExecuteTransactions {

	public ExecuteTransactions() {
		
	}
	
	private AllTransactionErrorsListener allTransactionErrorsListener;
	

	public ITransaction execute(DBConnection connection,TransactionRunnable tr){
		ITransaction t = ITransaction.getNewInstance(connection,true,true);
		t.execRunnable(tr);
		return t;
	}
	
	public ITransaction execute(DBConnection connection,TransactionRunnable tr,boolean recoverable,boolean schedulable){
		ITransaction t = ITransaction.getNewInstance(connection,recoverable,schedulable);
		t.execRunnable(tr);
		return t;
	}
	
	public ITransaction begin(DBConnection connection){
		return begin(connection,true);
	}
	
	public ITransaction begin(DBConnection connection, boolean recoverable){
		ITransaction t = ITransaction.getNewInstance(connection,recoverable,true);
		return t;
	}
	public ITransaction begin(DBConnection connection, boolean recoverable,boolean schedulable){
		ITransaction t = ITransaction.getNewInstance(connection,recoverable,schedulable);
		return t;
	}
	
	
	public AllTransactionErrorsListener getAllTransactionErrorsListener() {
		return allTransactionErrorsListener;
	}

	public void setAllTransactionErrorsListener(AllTransactionErrorsListener allTransactionErrorsListener) {
		this.allTransactionErrorsListener = allTransactionErrorsListener;
	}

	public static interface AllTransactionErrorsListener{
		void onFail(ITransaction transaction, Exception e);
	}
	
}
