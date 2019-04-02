package tests;

import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Transaction;

public interface Callback{
	Transaction call(Transaction t);
}