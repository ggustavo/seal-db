package DBMS.bufferManager;

import DBMS.bufferManager.policies.AbstractBufferPolicy;
import DBMS.transactionManager.ITransaction;

public interface IBufferManager {

	AbstractBufferPolicy getBufferPolicy();

	void setBufferPolicy(AbstractBufferPolicy bufferPolicy);

	IPage getPage(ITransaction transaction, String pageId, char operation, boolean isTemp);

	public void flush();

	void freeAll();
	
	void resetStatistics();
	
	void startTempBufferPolicy();
	
	int getColdDataState();
	
	void insertInCold(IPage page);
	
	IPage removeFromCold(String id);
	
	void saveColdData();

	static IBufferManager getInstance(){
		return new BufferManager();
	}

	public void setEnableColdDataLoad(boolean enableColdList);

	
}