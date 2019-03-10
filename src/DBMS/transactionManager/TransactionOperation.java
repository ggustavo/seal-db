package DBMS.transactionManager;

import DBMS.fileManager.ObjectDatabaseId;

public class TransactionOperation {

	public static final char READ_TRANSACTION = 'R';
	public static final char WRITE_TRANSACTION = 'W';
	public static final char ABORT_TRANSACTION = 'A';
	public static final char COMMIT_TRANSACTION = 'C';
	public static final char BEGIN_TRANSACTION = 'B';

	private ObjectDatabaseId objectDatabaseId;
	private char type;
	private ITransaction transaction;
	private byte[] beforeImage;
	private byte[] afterImage;

	public TransactionOperation(ITransaction transaction,ObjectDatabaseId idObject, char type) {
		this.transaction = transaction;
		this.objectDatabaseId = idObject;
		this.type = type;
	}
	
	public String toString(){
		return type+"("+objectDatabaseId+")";
	}
	
	public ITransaction getTransaction() {
		return transaction;
	}

	public void setTransaction(ITransaction transaction) {
		this.transaction = transaction;
	}


	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public ObjectDatabaseId getObjectDatabaseId() {
		return objectDatabaseId;
	}

	public void setObjectDatabaseId(ObjectDatabaseId objectDatabaseId) {
		this.objectDatabaseId = objectDatabaseId;
	}

	public byte[] getBeforeImage() {
		return beforeImage;
	}

	public void setBeforeImage(byte[] beforeImage) {
		this.beforeImage = beforeImage;
	}

	public byte[] getAfterImage() {
		return afterImage;
	}

	public void setAfterImage(byte[] afterImage) {
		this.afterImage = afterImage;
	}
	
	
	
	
}
