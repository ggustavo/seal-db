package DBMS.distributed;

import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.connectionManager.ResponseMenssageListener;
import DBMS.distributed.resourceManager.message.MessageHeader;

public class ResourceManagerConnection {
	
	
	private DBConnection dbConnection;
	private DistributedTransactionManagerController tm;
	private InetAddress address;
	private int port;
	private boolean closeConnectionFlag;
	private Branch remoteTransaction;

	public ResourceManagerConnection(DBConnection dbConnection, DistributedTransactionManagerController server, InetAddress address, int port) {
		super();
		this.dbConnection = dbConnection;
		this.tm = server;
		
		this.address = address;
		this.port = port;
		closeConnectionFlag = false;
	}
	public DBConnection getDbConnection() {
		return dbConnection;
	}
	public void setDbConnection(DBConnection dbConnection) {
		this.dbConnection = dbConnection;
	}
	public DistributedTransactionManagerController getTransactionManager() {
		return tm;
	}
	public void setServer(DistributedTransactionManagerController server) {
		this.tm = server;
	}
	
	public Branch getRemoteTransaction() {
		return remoteTransaction;
	}
	public void setRemoteTransaction(Branch remoteTransaction) {
		this.remoteTransaction = remoteTransaction;
	}
	public InetAddress getAddress() {
		return address;
	}
	
	public int getPort() {
		return port;
	}
	
	public Branch createBranch(String transactionGlobalID){
		
		if(remoteTransaction!=null)return null;
		remoteTransaction = new Branch(this);
		remoteTransaction.setTransactionGlobalID(transactionGlobalID);
		return remoteTransaction;
	}
	
	public boolean unRegister(){
		
		try {
			final CountDownLatch latch = new CountDownLatch(1);
		
			tm.getDispatcher().sendResquest(address, port, dbConnection, MessageHeader.CLOSE_CONNECTION, new ResponseMenssageListener<String>() {

				@Override
				public void onReceiver(String o) {
					closeConnectionFlag = true;
					latch.countDown();
					
					
				}

				@Override
				public void onErro(String e) {
					Kernel.log(this.getClass(),e,Level.SEVERE);
					closeConnectionFlag = false;
					latch.countDown();
					
				}

				@Override
				public Class<String> responseDataClass(){
					return String.class;
				}
			}, DistributedTransactionManagerController.TRANSACTION_MANAGER_TIMEOUT);
			
			latch.await();
		} catch (InterruptedException e) {
		
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return closeConnectionFlag;
	}
	
	
	
}
