package DBMS.transactionManager;

import DBMS.fileManager.ObjectDatabaseId;

public class Lock {
		
	
	public static final char WRITE_LOCK = 'W';
	public static final char READ_LOCK = 'R';
	
	private ObjectDatabaseId objectDatabaseId;
	private char lockType;
	private ITransaction transaction;
	private boolean canceled;
	
	public Lock(ITransaction transaction,ObjectDatabaseId objectDatabaseId, char lockType){
		this.objectDatabaseId = objectDatabaseId;
		this.lockType = lockType;
		this.transaction = transaction;
		canceled = false;
	}
	
	
	
	
	public ObjectDatabaseId getObjectDatabaseId() {
		return objectDatabaseId;
	}




	public void setObjectDatabaseId(ObjectDatabaseId objectDatabaseId) {
		this.objectDatabaseId = objectDatabaseId;
	}




	public char getLockType() {
		return lockType;
	}

	public void setLockType(char lockType) {
		this.lockType = lockType;
	}


	public ITransaction getTransaction() {
		return transaction;
	}


	public void setTransaction(ITransaction transaction) {
		this.transaction = transaction;
	}




	public boolean isCanceled() {
		return canceled;
	}




	public void setCanceled(boolean canceled) {
		this.canceled = canceled;
	}
	
	
	
}
