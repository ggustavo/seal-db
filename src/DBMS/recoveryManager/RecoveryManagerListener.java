package DBMS.recoveryManager;

import DBMS.fileManager.dataAcessManager.file.log.FileRecord;

public interface RecoveryManagerListener {
	void newRecord(FileRecord fileRecord);
}
