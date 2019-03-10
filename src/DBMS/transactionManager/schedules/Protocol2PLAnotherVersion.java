package DBMS.transactionManager.schedules;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;

import DBMS.fileManager.ObjectDatabaseId;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.Lock;
import DBMS.transactionManager.LockManager;
import DBMS.transactionManager.TransactionManagerListener;
import DBMS.transactionManager.TransactionOperation;

public class Protocol2PLAnotherVersion extends AbstractScheduler{

	
	public  void unlockAll(ITransaction transaction){
		

		List<Lock> locks = transaction.getLockList();
		TransactionManagerListener tl = Kernel.getTransactionManagerListener();
		
		for (Lock lock : locks) {
			LockManager lockManager = lockMap.get(lock.getObjectDatabaseId());
			if(lockManager!=null){
				
				List<Lock> lockList = lockManager.getLockList();
				List<Lock> lockWaitList = lockManager.getLockWaitList();	
				lockList.remove(lock);
				lockWaitList.remove(lock);
				if(lockList.isEmpty() && lockWaitList.isEmpty()){
					lockMap.remove(lock.getObjectDatabaseId());
					if(tl!=null)tl.unLock(lock);
				}
				
			}
		}
	
		waitForGraph.removeNode(waitForGraph.findNode(transaction));
		
	}
	
	
	public Lock requestLock(ITransaction transaction, TransactionOperation transactionOperation) throws InterruptedException{
		
	//	print();
		Lock newLock = createLock(transaction,transactionOperation);
		
		if(lockMap.containsKey(newLock.getObjectDatabaseId())){
			
			LockManager lockManager = lockMap.get(newLock.getObjectDatabaseId());
			List<Lock> lockList = lockManager.getLockList();
			List<Lock> lockWaitList = lockManager.getLockWaitList();
			Lock lockSameTransaction = null;
		
		
			for (Lock l : lockList) {
				if(l.getTransaction() == newLock.getTransaction()){
					lockSameTransaction = l;
					break;
				}
			}
			
			if(lockSameTransaction!=null){
				
				lockSameTransaction(newLock, lockSameTransaction);
			
			}else{
				
				List<Lock> lockConflitList = findConflit(newLock, lockList);
				
				while(lockConflitList != null){
					 lockWaitList.add(newLock);
					
						for (Lock l : lockConflitList) {
							
							waitForGraph.addEdge(newLock.getTransaction(), l.getTransaction());
							Kernel.log(this.getClass(),"Transaction " + newLock.getTransaction().getIdT() + " wait for " + l.getTransaction().getIdT(),Level.WARNING);
							
							if(waitForGraph.hasCycle(transaction)){
								
								Kernel.log(this.getClass(),"FIND! CYCLE",Level.WARNING);
								Kernel.log(this.getClass(),"Abort Transaction: " + newLock.getTransaction().getIdT(),Level.WARNING);
								newLock.getTransaction().abort();
								return null;
							}
						}
						waitTransaction(transaction);
				}
				
				lockList.add(newLock);					
		
				
			}
			
			
			
		}else{
			
			List<Lock> lockList = new LinkedList<>();
			lockList.add(newLock);
			lockMap.put(newLock.getObjectDatabaseId(), new LockManager(lockList, new LinkedList<>()));	
			
			
		}
		
		
		return newLock;
	}


	private synchronized List<Lock> findConflit(Lock newLock, List<Lock> lockList) {
		List<Lock> lockConflitList = new LinkedList<>();
		for (Lock l : lockList) {
			if(l.getTransaction() != newLock.getTransaction()){
				if(newLock.getLockType() == Lock.WRITE_LOCK || l.getLockType() == Lock.WRITE_LOCK){
					lockConflitList.add(l);	
				}
			}
		}
		if(lockConflitList.isEmpty())return null;
		
		return lockConflitList;
	}
	
	
	private void lockSameTransaction(Lock newLock, Lock lockSameTransaction){
		if(newLock.getLockType() == Lock.WRITE_LOCK){
			lockSameTransaction.setLockType(Lock.WRITE_LOCK);
			
			if(Kernel.getTransactionManagerListener()!=null){
				Kernel.getTransactionManagerListener().updateLock(newLock);
			}
		}
	}
	
	
	public void waitTransaction(ITransaction transaction) throws InterruptedException{
	
			synchronized (transaction.getThread()) {
				transaction.setState(ITransaction.WAIT);
				transaction.getThread().wait();						
			}
			
		
	}


	@Override
	public void unlock(ObjectDatabaseId objectDatabaseId) {
		// TODO Auto-generated method stub
		
	}
}
