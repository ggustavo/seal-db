package DBMS.transactionManager;



import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
import DBMS.recoveryManager.IRecoveryManager;

public class Transaction implements ITransaction{

	private boolean recoverable = true;
	private boolean schedulable = true;
	
	private LinkedHashSet<TransactionOperation> operations;
	private List<Lock> lockList;
	

	private int idT;
	private char state = ACTIVE;
	private Thread thread;
	private boolean storageHistory = false;

	private DBConnection connection;


	public Transaction(DBConnection connection, boolean recoverable, boolean schedulable) {

		if(!Kernel.getScheduler().isAbortAllProcess()){
			this.schedulable = schedulable;
			this.recoverable = recoverable;
			this.connection = connection;
			connection.getTransactions().add(this);
			if(schedulable){
				idT = Kernel.getNewID(Kernel.PROPERTIES_TRASACTION_ID);
				lockList = new LinkedList<>();
				operations = (new LinkedHashSet<TransactionOperation> ());
				operations = null;
				Kernel.getScheduler().getWaitForGraph().addNode(this);			
				addOperation( TransactionOperation.BEGIN_TRANSACTION);				
			}
		}
	}
	
	
	public DBConnection getConnection() {
		return connection;
	}
	
	
	public TransactionOperation  lock(ObjectDatabaseId obj, char type){
		if(!schedulable)return new TransactionOperation(this, obj, type);
		if(!canExec())return null;
		TransactionOperation transactionOperation = new TransactionOperation(this, obj, type);
		if(storageHistory) operations.add(transactionOperation);
		try {
			TransactionManagerListener t = Kernel.getTransactionManagerListener();
			if(t!=null)t.newTransactionOperation(transactionOperation);
			
			Lock lock = Kernel.getScheduler().requestLock(this, transactionOperation);
			if(t!=null)t.newLock(lock);
			if(!lock.isCanceled()){
				lockList.add(lock);
				if(t!=null)t.newTransactionOperationScheduled(transactionOperation);				
			}else{
				return null;
			}
		
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
		return transactionOperation;
	}
	
	public void unlock(IPage page,ObjectDatabaseId obj, TransactionOperation operation, byte[] beforeImage, byte[] afterImage, boolean isTemp){
		if(!schedulable)return;
		if(afterImage != null && beforeImage != null && operation.getType() == TransactionOperation.WRITE_TRANSACTION && recoverable && !isTemp){
			operation.setBeforeImage(beforeImage);
			operation.setAfterImage(afterImage);
			int lsn = Kernel.getRecoveryManager().appendOrUndoTransaction(operation,this,IRecoveryManager.ACTION_APPEND_OPERATION);
			FileBlock fileBlock = new FileBlock(page.getData());
			fileBlock.setLSN(lsn);
			page.setData(fileBlock.getBlock());
		}
		Kernel.getScheduler().unlock(obj);
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
						//setState(Partially_Committed);
						thread = null;
					
					}catch(Exception e){
						Kernel.exception(this.getClass(),e);
						tr.onFail(Transaction.this, e);
						if(Kernel.getExecuteTransactions().getAllTransactionErrorsListener() != null) {
							Kernel.getExecuteTransactions().getAllTransactionErrorsListener().onFail(Transaction.this, e);
						}
						thread = null;
							
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
		TransactionManagerListener t = Kernel.getTransactionManagerListener();
		if(recoverable)Kernel.getRecoveryManager().appendOrUndoTransaction(null,this,IRecoveryManager.ACTION_UNDO_TRASACTION);
		addOperation(TransactionOperation.ABORT_TRANSACTION);
		unlockAll();
		if(t!=null)t.transactionFailed(Transaction.this);
	}
	
	public void commit() {
		if(!schedulable)return;
		if(!canExec())return;
		setState(COMMITTED);
		TransactionManagerListener t = Kernel.getTransactionManagerListener();
		addOperation(TransactionOperation.COMMIT_TRANSACTION);
		unlockAll();
		if (t != null)t.transactionCommit(Transaction.this);

	}
	
	public void abort(){
		if(!schedulable)return;
		if(!canExec())return;
		setState(ABORTED);
		TransactionManagerListener t = Kernel.getTransactionManagerListener();
		if(recoverable)Kernel.getRecoveryManager().appendOrUndoTransaction(null,this,IRecoveryManager.ACTION_UNDO_TRASACTION);
		addOperation(TransactionOperation.ABORT_TRANSACTION);
		unlockAll();
		if(t!=null)t.transactionAbort(this);
	}
	
	public void rollback(){
		if(!schedulable)return;
		if(!canExec())return;
		//TODO still to be done ...
	}
	
	private void addOperation(char type){
		if(!schedulable)return;
		TransactionOperation operation = new TransactionOperation(this, null, type);
		if(storageHistory) operations.add(operation);
		if(recoverable)Kernel.getRecoveryManager().appendOrUndoTransaction(operation,this,IRecoveryManager.ACTION_APPEND_OPERATION);			
		
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

	public LinkedHashSet<TransactionOperation> getOperations() {
		return operations;
	}

	public void setOperations(LinkedHashSet<TransactionOperation> operations) {
		this.operations = operations;
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
			Kernel.log(this.getClass(),"Transaction Finish, code: " + state,Level.WARNING);
			return false;
		}
	}


	public boolean isSchedulable() {
		return schedulable;
	}


	public void setScalable(boolean schedulable) {
		this.schedulable = schedulable;
	}


	@Override
	public boolean isFinish() {
		if(state == COMMITTED || state == FAILED || state == ABORTED) {
			return true;
		}
		return false;
	}


	public void setIdT(int idT) {
		this.idT = idT;
	}


	public boolean isStorageHistory() {
		return storageHistory;
	}


	public void setStorageHistory(boolean storageHistory) {
		this.storageHistory = storageHistory;
	}
	
	
	
}
