package DBMS.queryProcessing;

import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.TransactionRunnable;

public class ExecuteTransactions {

	public ExecuteTransactions() {
		
	}
	
	private AllTransactionErrorsListener allTransactionErrorsListener;
	

	public Transaction execute(TransactionRunnable tr){
		Transaction t = Transaction.getNewInstance(true,true);
		t.execRunnable(tr);
		return t;
	}
	
	public Transaction execute(TransactionRunnable tr,boolean recoverable,boolean schedulable){
		Transaction t = Transaction.getNewInstance(recoverable,schedulable);
		t.execRunnable(tr);
		return t;
	}
	
	public Transaction begin(){
		return begin(true);
	}
	
	public Transaction begin(boolean recoverable){
		Transaction t = Transaction.getNewInstance(recoverable,true);
		return t;
	}
	public Transaction begin(boolean recoverable,boolean schedulable){
		Transaction t = Transaction.getNewInstance(recoverable,schedulable);
		return t;
	}
	
	
	public Transaction execute(TransactionRunnable tr,boolean recoverable,boolean schedulable, boolean noThread){
		Transaction t = Transaction.getNewInstance(recoverable,schedulable);
		t.setNoThread(noThread);
		t.execRunnable(tr);
		return t;
	}
	
	public AllTransactionErrorsListener getAllTransactionErrorsListener() {
		return allTransactionErrorsListener;
	}

	public void setAllTransactionErrorsListener(AllTransactionErrorsListener allTransactionErrorsListener) {
		this.allTransactionErrorsListener = allTransactionErrorsListener;
	}

	public static interface AllTransactionErrorsListener{
		void onFail(Transaction transaction, Exception e);
	}
	
}
