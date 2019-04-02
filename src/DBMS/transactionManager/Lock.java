package DBMS.transactionManager;

import DBMS.queryProcessing.Tuple;

public class Lock {

	public static final char WRITE_LOCK = 'W';
	public static final char READ_LOCK = 'R';

	private Tuple tuple;
	private char lockType;
	private Transaction transaction;
	private boolean canceled;

	public Lock(Transaction transaction, Tuple TupleManipulate, char lockType) {
		this.tuple = TupleManipulate;
		this.lockType = lockType;
		this.transaction = transaction;
		canceled = false;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public void setTuple(Tuple TupleManipulate) {
		this.tuple = TupleManipulate;
	}

	public char getLockType() {
		return lockType;
	}

	public void setLockType(char lockType) {
		this.lockType = lockType;
	}

	public Transaction getTransaction() {
		return transaction;
	}

	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}

	public boolean isCanceled() {
		return canceled;
	}

	public void setCanceled(boolean canceled) {
		this.canceled = canceled;
	}

}
