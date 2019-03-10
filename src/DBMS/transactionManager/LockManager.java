package DBMS.transactionManager;

import java.util.List;

public class LockManager {
	private List<Lock> lockList;
	private List<Lock> lockWaitList;
	
	public LockManager(List<Lock> lockList, List<Lock> lockWaitList) {
		super();
		this.lockList = lockList;
		this.lockWaitList = lockWaitList;
	}
	
	public List<Lock> getLockList() {
		return lockList;
	}
	public void setLockList(List<Lock> lockList) {
		this.lockList = lockList;
	}
	public List<Lock> getLockWaitList() {
		return lockWaitList;
	}
	public void setLockWaitList(List<Lock> lockWaitList) {
		this.lockWaitList = lockWaitList;
	}
	
	
}
