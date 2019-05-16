package DBMS.transactionManager;



import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;

public class Transaction {

	public static final char ACTIVE = '1';
	public static final char PREPARED = '2';
	public static final char FAILED = '3';
	public static final char ABORTED = '4';
	public static final char COMMITTED = '5';
	public static final char WAIT = '6';
	
	
	private boolean recoverable = true;
	private boolean schedulable = true;
	
	private LinkedList<TransactionOperation> operations;
	//private List<Lock> lockList;
	private List<MTable> temps;

	private int idT;
	private char state = ACTIVE;
	private Runnable threadRunnable;

	public static int TRANSACTION_COUNT = 0;
	
	public Transaction(boolean recoverable, boolean schedulable) {
		
		if (!Kernel.getScheduler().isAbortAllProcess()) {
			this.schedulable = schedulable;
			this.recoverable = recoverable;
			idT = Kernel.getNewID(Kernel.PROPERTIES_TRASACTION_ID);
		//	lockList = Collections.synchronizedList(new LinkedList<Lock>()); 
			operations = (new LinkedList<TransactionOperation>());
		//	Kernel.getScheduler().getWaitForGraph().addNode(this);
		//	operations.add(new TransactionOperation(this, null, TransactionOperation.BEGIN_TRANSACTION));
			temps = new LinkedList<MTable>();
			
		}
	}
	
	public static Transaction getNewInstance(boolean recoverable,boolean schedulable){
		return new Transaction(recoverable,schedulable);
	}
	public static Transaction getNewInstance(){
		return new Transaction(true,true);
	}
	
	public ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	
	public boolean lock(Tuple obj, char type) throws AcquireLockException{
		if(!schedulable)return true;
		if(!canExec())return false;
		
		if(true)return true;
		
		if(obj.getTable().isTemp()) {
			return true;
		}
		
		if(type == Lock.READ_LOCK) {
			return true;
		}
		
		
		if(obj.isUsed == false){
			obj.isUsed = true;
			tuples.add(obj);
			obj.transactionId = getIdT();
			return true;
		}else if(obj.transactionId == getIdT()){
			return true;
		}
		
		
		
		throw new AcquireLockException();
	}
	
	
	
	public void unlock(char operationReadOrWrite, char tupleOperation, Tuple tuple, String[] beforedata, boolean isTemp){
		
		if(isTemp && !temps.contains(tuple.getTable())){
			temps.add(tuple.getTable());
		}
		
		if(!isTemp && recoverable && operationReadOrWrite == TransactionOperation.WRITE_TUPLE) {
			TransactionOperation operation = new TransactionOperation(this, tuple, Lock.WRITE_LOCK);	
			operation.seTupleOperation(tupleOperation);
			operation.setBeforedata(beforedata);
			operations.add(operation);
		}
		
		//if(schedulable)Kernel.getScheduler().unlock(tuple);
	}
	
	public void unlockAll(){
		if(!schedulable)return;
		
		for (Tuple tuple : tuples) {
			tuple.isUsed = false;
			tuple.transactionId = -1;
		}
		//Kernel.getScheduler().unlockAll(this);
	}
	

	
	public boolean execRunnable(TransactionRunnable tr){
		if(!canExec())return false;
		if(threadRunnable == null || !schedulable){
			
			threadRunnable = new Runnable() {
				
				@Override
				public void run() {
					
					try{		
						tr.run(Transaction.this);
						threadRunnable = null;
					
					}catch(Exception e){
						//Kernel.exception(this.getClass(),e);
						tr.onFail(Transaction.this, e);
						if(Kernel.getExecuteTransactions().getAllTransactionErrorsListener() != null) {
							Kernel.getExecuteTransactions().getAllTransactionErrorsListener().onFail(Transaction.this, e);
						}
						threadRunnable = null;
						return;
							
					}
					
				}
			};
			
			Kernel.TRANSACTIONS_EXECUTOR.execute(threadRunnable);
			
			//new Thread(threadRunnable).start();
			
		}else{
			if(schedulable)Kernel.log(this.getClass(),"Unfinished operations running",Level.SEVERE);
			return false;
		}
		
		return true;
	}
	
	public void failed(){
		TRANSACTION_COUNT++;
		if(!schedulable)return;
		if(!canExec())return;
		setState(FAILED);
		//operations.add(new TransactionOperation(this, null, TransactionOperation.ABORT_TRANSACTION));
		if(recoverable)Kernel.getRecoveryManager().undoTransaction(this);
		clearTempTables();
		unlockAll();
	}
	
	public void commit() {
		TRANSACTION_COUNT++;
		if(!schedulable)return;
		if(!canExec())return;
		setState(COMMITTED);
		//operations.add(new TransactionOperation(this, null, TransactionOperation.COMMIT_TRANSACTION));
		if(recoverable)Kernel.getRecoveryManager().commitTransaction(this);
		clearTempTables();
		unlockAll();
	}
	
	public void abort(){
		TRANSACTION_COUNT++;
		if(!schedulable)return;
		if(!canExec())return;
		setState(ABORTED);
		//operations.add(new TransactionOperation(this, null, TransactionOperation.ABORT_TRANSACTION));
		if(recoverable)Kernel.getRecoveryManager().undoTransaction(this);
		clearTempTables();
		unlockAll();
	}

	
	public void clearTempTables(){
		for (MTable table : temps) {
			table.getSchemaManipulate().removeTable(table);
		}
	}

	public int getIdT() {
		return idT;
	}

	public char getState() {
		return state;
	}

	public void setState(char state) {
		this.state = state;
	}

	public LinkedList<TransactionOperation> getOperations() {
		return operations;
	}
	
//	public List<Lock> getLockList() {
//		return lockList;
//	}
//
//	public void setLockList(List<Lock> lockList) {
//		this.lockList = lockList;
//	}
	public Runnable getThread() {
		return threadRunnable;
	}

	public void setThread(Runnable thread) {
		this.threadRunnable = thread;
	}

	public boolean isRecoverable() {
		return recoverable;
	}

	public void setRecoverable(boolean recoverable) {
		this.recoverable = recoverable;
	}
	
	public boolean canExec(){
	
		if(state == ACTIVE || state == PREPARED){
			return true;
		}else{
			Kernel.log(this.getClass(),"Transaction Finish, state: " + getState(state),Level.WARNING);
			return false;
		}
	}

	public static String getState(char state) {
		if(state == ACTIVE)return "ACTIVE";
		if(state == PREPARED)return "PREPARED";
		if(state == FAILED)return "FAILED";
		if(state == ABORTED)return "ABORTED";
		if(state == COMMITTED)return "COMMITTED";
		if(state ==  WAIT )return "WAIT";
		return null;
	}

	public boolean isSchedulable() {
		return schedulable;
	}


	public void setScalable(boolean schedulable) {
		this.schedulable = schedulable;
	}


	public boolean isFinish() {
		if(state == COMMITTED || state == FAILED || state == ABORTED) {
			return true;
		}
		return false;
	}


	public void setIdT(int idT) {
		this.idT = idT;
	}


	
	
}
