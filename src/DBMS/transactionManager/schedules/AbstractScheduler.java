package DBMS.transactionManager.schedules;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.Lock;
import DBMS.transactionManager.LockManager;
import DBMS.transactionManager.TransactionOperation;
import DBMS.transactionManager.serializationGraph.TransactionNode;
import DBMS.transactionManager.serializationGraph.WaitForGraph;


public abstract class AbstractScheduler {

	protected HashMap<ObjectDatabaseId, LockManager> lockMap;
	protected WaitForGraph waitForGraph;
	protected boolean abortAllProcess = false;

	public AbstractScheduler() {
		lockMap = new HashMap<>();
		setWaitForGraph(new WaitForGraph());
	}
	public abstract void unlockAll(ITransaction transaction);
	public abstract void unlock(ObjectDatabaseId objectDatabaseId);
	public abstract Lock requestLock(ITransaction transaction, TransactionOperation transactionOperation) throws InterruptedException;
	
	protected Lock createLock(ITransaction transaction, TransactionOperation transactionOperation) {
		if (transactionOperation.getType() == TransactionOperation.READ_TRANSACTION) {
			return new Lock(transaction, transactionOperation.getObjectDatabaseId(), Lock.READ_LOCK);
		} else if (transactionOperation.getType() == TransactionOperation.WRITE_TRANSACTION) {
			return new Lock(transaction, transactionOperation.getObjectDatabaseId(), Lock.WRITE_LOCK);
		}
		return null;
	}

	public WaitForGraph getWaitForGraph() {
		return waitForGraph;
	}

	public void setWaitForGraph(WaitForGraph waitForGraph) {
		this.waitForGraph = waitForGraph;
	}
	
	public void print(){
		Kernel.log(this.getClass(),"-------------------",Level.INFO);
		Set<ObjectDatabaseId> keys = lockMap.keySet();
		for (ObjectDatabaseId k : keys) {
			LockManager l = lockMap.get(k);
			Kernel.log(this.getClass(),"Object: " + k,Level.INFO);
			List<Lock> locks = l.getLockList();
			Kernel.log(this.getClass(),"Atual Lock",Level.INFO);
			for (Lock lock : locks) {
				Kernel.log(this.getClass(),lock.getLockType() + " T"+lock.getTransaction().getIdT(),Level.INFO);
			}
			Kernel.log(this.getClass(),"Wait Lock",Level.INFO);
			locks = l.getLockWaitList();
			for (Lock lock : locks) {
				Kernel.log(this.getClass(),lock.getLockType() + " T"+lock.getTransaction().getIdT(),Level.INFO);
			}
		}
		Kernel.log(this.getClass(),"-------------------",Level.INFO);
	}
	
	public synchronized void abortAll(){
		abortAllProcess = true;
		List<TransactionNode> nodes = new LinkedList<>();
		
		for (TransactionNode node : waitForGraph.getNodes()) {
			nodes.add(node);
		}
		
		for (TransactionNode node : nodes) {
			node.getTransaction().abort();
		}
		abortAllProcess = false;
	}
	
	public boolean isAbortAllProcess() {
		return abortAllProcess;
	}
	
}
