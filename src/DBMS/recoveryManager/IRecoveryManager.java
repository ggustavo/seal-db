package DBMS.recoveryManager;

import java.util.Date;
import java.util.List;

import DBMS.fileManager.dataAcessManager.file.log.FileLog;
import DBMS.fileManager.dataAcessManager.file.log.FileRecord;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionOperation;

public interface IRecoveryManager {

	public static char ACTION_APPEND_OPERATION = 'A';
	public static char ACTION_UNDO_TRASACTION = 'U';

	void start(String path);

	FileLog getFileLogAcess();

	void printLog();

	void safeFinalize();

	int appendOrUndoTransaction(TransactionOperation operation, ITransaction transaction, char action);

	RecoveryManagerListener getRecoveryManagerListen();

	void setRecoveryManagerListen(RecoveryManagerListener recoveryManagerListen);

	List<FileRecord> findByDate(Date date1, Date date2);

	List<FileRecord> findByLSN(int LSN1, int LSN2);

	List<FileRecord> findByTransaction(int transaction);
	
	public static IRecoveryManager getInstance(){
		return new RecoveryManager();
	}
	

}