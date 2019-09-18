package tests;


import DBMS.transactionManager.Transaction;

public interface Callback{
	Transaction call(Transaction t);
}