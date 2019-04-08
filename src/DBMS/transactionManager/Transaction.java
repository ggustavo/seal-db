package DBMS.transactionManager;



import java.util.ArrayList;
import java.util.Collections;
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
	
	private boolean SEND_READ_OPERATIONS_TO_lOG = false;
	private boolean SEND_TEMP_OPERATIONS_TO_lOG = false;
	
	private LinkedList<TransactionOperation> operations;
	private List<Lock> lockList;
	private List<MTable> temps;

	private int idT;
	private char state = ACTIVE;
	private Thread thread;
	
	public static ArrayList<Transaction> transactionsList = new ArrayList<>();
	
	public Transaction(boolean recoverable, boolean schedulable) {
		
		if (!Kernel.getScheduler().isAbortAllProcess()) {
			this.schedulable = schedulable;
			this.recoverable = recoverable;
			idT = Kernel.getNewID(Kernel.PROPERTIES_TRASACTION_ID);
			transactionsList.add(this);
			lockList = Collections.synchronizedList(new LinkedList<Lock>()); 
			operations = (new LinkedList<TransactionOperation>());
			Kernel.getScheduler().getWaitForGraph().addNode(this);
			operations.add(new TransactionOperation(this, null, TransactionOperation.BEGIN_TRANSACTION));
			temps = new LinkedList<MTable>();
			
		}
	}
	
	public static Transaction getNewInstance(boolean recoverable,boolean schedulable){
		return new Transaction(recoverable,schedulable);
	}
	public static Transaction getNewInstance(){
		return new Transaction(true,true);
	}
	
	public TransactionOperation lock(Tuple obj, char type) throws AcquireLockException{
		if(!schedulable)return new TransactionOperation(this, obj, type);
		if(!canExec())return null;
		TransactionOperation transactionOperation = new TransactionOperation(this, obj, type);	
		
		try {
			Lock lock = Kernel.getScheduler().requestLock(this, transactionOperation);
			if(lock == null) {
				throw new AcquireLockException();
			}
			if(!lock.isCanceled()){
				lockList.add(lock);		
			}else {
				throw new AcquireLockException();
			}
		
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new AcquireLockException();
		}
		return transactionOperation;
	}
	
	
	
	public void unlock(char tupleOperation, TransactionOperation operation, Tuple tuple, String[] beforedata, boolean isTemp){
		
		operation.seTupleOperation(tupleOperation);
		if(recoverable && ((SEND_READ_OPERATIONS_TO_lOG && operation.getType() == TransactionOperation.READ_TUPLE ) || 
				(SEND_TEMP_OPERATIONS_TO_lOG && isTemp))) {
			operations.add(operation);			
		}
		
		
		
		if(SEND_TEMP_OPERATIONS_TO_lOG && recoverable && operation.getType() == TransactionOperation.WRITE_TUPLE) {
			operation.setBeforedata(beforedata);
		}
	
		if(isTemp && !temps.contains(tuple.getTable())){
			temps.add(tuple.getTable());
		}
		
		if(schedulable)Kernel.getScheduler().unlock(tuple);
	}
	
	public void unlockAll(){
		if(!schedulable)return;
		Kernel.getScheduler().unlockAll(this);
	}
	

	
	public boolean execRunnable(TransactionRunnable tr){
		if(!canExec())return false;
		if(thread == null || !schedulable){
			
			thread = new Thread(new Runnable() {
				public void run() {
				try{		
						tr.run(Transaction.this);
						thread = null;
					
					}catch(Exception e){
						Kernel.exception(this.getClass(),e);
						tr.onFail(Transaction.this, e);
						if(Kernel.getExecuteTransactions().getAllTransactionErrorsListener() != null) {
							Kernel.getExecuteTransactions().getAllTransactionErrorsListener().onFail(Transaction.this, e);
						}
						thread = null;
						return;
							
					}
					
				}
			});
			thread.start();
		}else{
			if(schedulable)Kernel.log(this.getClass(),"Unfinished operations running",Level.SEVERE);
			return false;
		}
		
		return true;
	}
	
	public void failed(){
		if(!schedulable)return;
		if(!canExec())return;
		setState(FAILED);
		operations.add(new TransactionOperation(this, null, TransactionOperation.ABORT_TRANSACTION));
		if(recoverable)Kernel.getRecoveryManager().undoTransaction(this);
		clearTempTables();
		unlockAll();
	}
	
	public void commit() {
		if(!schedulable)return;
		if(!canExec())return;
		setState(COMMITTED);
		operations.add(new TransactionOperation(this, null, TransactionOperation.COMMIT_TRANSACTION));
		if(recoverable)Kernel.getRecoveryManager().commitTransaction(this);
		clearTempTables();
		unlockAll();
	}
	
	public void abort(){
		if(!schedulable)return;
		if(!canExec())return;
		setState(ABORTED);
		operations.add(new TransactionOperation(this, null, TransactionOperation.ABORT_TRANSACTION));
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
	
	public List<Lock> getLockList() {
		return lockList;
	}

	public void setLockList(List<Lock> lockList) {
		this.lockList = lockList;
	}
	public Thread getThread() {
		return thread;
	}

	public void setThread(Thread thread) {
		this.thread = thread;
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
