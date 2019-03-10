package DBMS.distributed;

import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.connectionManager.Dispatcher;
import DBMS.connectionManager.ResponseMenssageListener;
import DBMS.distributed.commitprotocols.TwoPhaseProtocol;
import DBMS.distributed.resourceManager.ConnectionService;
import DBMS.distributed.resourceManager.PersistenceMessageService;
import DBMS.distributed.resourceManager.TransactionService;
import DBMS.distributed.resourceManager.message.MessageHeader;
import DBMS.distributed.resourceManager.message.types.LogTransactionMessage;
import DBMS.distributed.resourceManager.message.types.NewConnectionMessage;



public class DistributedTransactionManagerController {
	
	
	public static final int TRANSACTION_MANAGER_TIMEOUT = 3000;

	private static DistributedTransactionManagerController tm;
	private static Dispatcher dispatcher;
	
	//*Local Resource Manager Services
	private ConnectionService connectionService;
	private TransactionService transactionService;
	private PersistenceMessageService persistenceMessageService;
	
	public static DistributedTransactionManagerController getInstance(){
		if(tm == null){
			tm = new DistributedTransactionManagerController();
			tm.start();
			
		}
		return tm;
	}
	
	public void start(){
		dispatcher = new Dispatcher(this);
		startResorceServices();
	}
	private void startResorceServices(){
		connectionService = new ConnectionService(dispatcher);
		transactionService = new TransactionService(dispatcher);
	}
	
	public void startPersistenceMessageService(){
		persistenceMessageService = new PersistenceMessageService(dispatcher);
	}
	
	public ResourceManagerConnection register(InetAddress address, int port,String url, String username,String password){
		
		final CountDownLatch latch = new CountDownLatch(1);
		
		NewConnectionMessage newConnectionMessage = new NewConnectionMessage(url, username, password);
		final ResourceManagerConnection[] remoteDBConnection = new ResourceManagerConnection[1];
		try {
		
			dispatcher.sendResquest(address, port, newConnectionMessage, MessageHeader.NEW_CONNECTION,
					new ResponseMenssageListener<DBConnection>() {

						public void onReceiver(DBConnection c) {
							remoteDBConnection[0] = new ResourceManagerConnection(c, DistributedTransactionManagerController.getInstance(),address,port);
							latch.countDown();
						}

						public void onErro(String e) {
							Kernel.log(this.getClass(),e,Level.SEVERE);
							latch.countDown();
						}

						public Class<DBConnection> responseDataClass() {
							return DBConnection.class;
						}

					}, TRANSACTION_MANAGER_TIMEOUT);
				
		
	
		latch.await();
		
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			e.printStackTrace();
		} 
		return remoteDBConnection[0];
	}
	
	
	public void logMessageTransactionBranchs(ArrayList<ResourceManagerConnection> resourcesManagerConnection){
		final CountDownLatch latch = new CountDownLatch(1);
		
		LogTransactionMessage log = new LogTransactionMessage();
		try {
			for (ResourceManagerConnection r : resourcesManagerConnection){
				log.getParticipantsIP().add(r.getAddress().getHostAddress()+":"+r.getPort());
				log.setLeaderIP(InetAddress.getLocalHost().getHostAddress());
			}
		
			for (ResourceManagerConnection r : resourcesManagerConnection){
				log.setTransactionID(r.getRemoteTransaction().getTransactionGlobalID());
				dispatcher.sendResquest(r.getAddress(), r.getPort(), log, MessageHeader.LOG_TRANSACTION,
						new ResponseMenssageListener<DBConnection>() {
	
							public void onReceiver(DBConnection c) {
								latch.countDown();
							}
	
							public void onErro(String e) {
								Kernel.log(this.getClass(),e,Level.SEVERE);
								latch.countDown();
							}
	
							public Class<DBConnection> responseDataClass() {
								return DBConnection.class;
							}
	
						}, TRANSACTION_MANAGER_TIMEOUT);
			}		
			
			latch.await();
			
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SocketTimeoutException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} 
	}
	
	public void commit(ArrayList<ResourceManagerConnection> resourcesManagerConnection) throws UnknownHostException{
		new TwoPhaseProtocol(resourcesManagerConnection).start();
	}
	
	public DBConnection getLocalConnection(String url, String username,String password) {
		return connectionService.getLocalConnection(url, username, password);
	}

	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	public ConnectionService getConnectionService() {
		return connectionService;
	}

	
	public TransactionService getTransactionService() {
		return transactionService;
	}

	
	public PersistenceMessageService getPersistenceMessageService() {
		return persistenceMessageService;
	}

	

}