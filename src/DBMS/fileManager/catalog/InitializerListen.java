package DBMS.fileManager.catalog;

import DBMS.transactionManager.Transaction;

public interface InitializerListen {
	public void afterStartSystemCatalog(Transaction systemTransaction);
}
