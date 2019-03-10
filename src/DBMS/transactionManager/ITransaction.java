package DBMS.transactionManager;

import java.util.LinkedHashSet;
import java.util.List;

import DBMS.bufferManager.IPage;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.ObjectDatabaseId;

public interface ITransaction {

	public static char ACTIVE = '1';
	public static char PREPARED = '2';
	public static char FAILED = '3';
	public static char ABORTED = '4';
	public static char COMMITTED = '5';
	public static char WAIT = '6';

	DBConnection getConnection();

	TransactionOperation lock(ObjectDatabaseId obj, char type);

	void unlock(IPage page, ObjectDatabaseId obj, TransactionOperation operation, byte[] beforeImage, byte[] afterImage,boolean isTemp);

	void unlockAll();

	boolean execRunnable(TransactionRunnable tr);

	void failed();

	void commit();

	void abort();

	void rollback();

	int getIdT();

	char getState();

	void setState(char state);

	LinkedHashSet<TransactionOperation> getOperations();

	void setOperations(LinkedHashSet<TransactionOperation> operations);

	List<Lock> getLockList();

	void setLockList(List<Lock> lockList);

	Thread getThread();

	void setThread(Thread thread);

	boolean isRecoverable();

	void setRecoverable(boolean recoverable);

	boolean canExec();

	boolean isSchedulable();

	void setScalable(boolean schedulable);
	
	static ITransaction getNewInstance(DBConnection connection, boolean recoverable,boolean schedulable){
		return new Transaction(connection,recoverable,schedulable);
	}
	static ITransaction getNewInstance(DBConnection connection){
		return new Transaction(connection,true,true);
	}
	
	boolean isFinish();
}