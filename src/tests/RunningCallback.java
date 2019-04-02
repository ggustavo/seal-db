package tests;

import DBMS.queryProcessing.queryEngine.AcquireLockException;

public interface RunningCallback {
	void run() throws AcquireLockException;
}
