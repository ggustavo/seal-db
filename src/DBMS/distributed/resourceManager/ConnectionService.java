package DBMS.distributed.resourceManager;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.connectionManager.Dispatcher;
import DBMS.distributed.resourceManager.message.MessageHeader;
import DBMS.distributed.resourceManager.message.types.NewConnectionMessage;
import DBMS.transactionManager.ITransaction;

public class ConnectionService {
	
	public static final int CONNECTION_SERVICE_TIMEOUT = 3000;
	public static int MAX_CONNECTIONS = 100; 
	private static HashMap<String, DBConnection> connectionsPool;
	private Dispatcher dispatcher;
	
	public ConnectionService(Dispatcher dispatcher){
		this.dispatcher = dispatcher;
		connectionsPool = new HashMap<String, DBConnection>();
	}
	
	
	public void createConnection(MessageHeader mh){
		
		NewConnectionMessage n = mh.getData(NewConnectionMessage.class);
		//Test Shema Exist
		
		DBConnection dbC = getLocalConnection(n.getUrl(), n.getUsername(), n.getPassword());
		
		try {
			dispatcher.sendResponseRequest(mh, dbC, CONNECTION_SERVICE_TIMEOUT );
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	public void closeConnection(MessageHeader mh){
		
		try {
		DBConnection d = mh.getData(DBConnection.class);
		if(closeConnection(String.valueOf(d.getId()))){
				dispatcher.sendResponseRequest(mh, "Connection ID: " + d.getId() + " closed",CONNECTION_SERVICE_TIMEOUT );
			
		}else{
			mh.fault = "[ERR0] Connection "+d.getId()+" not found";
			dispatcher.sendResponseRequest(mh, mh.fault,CONNECTION_SERVICE_TIMEOUT );
		}
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	
	public DBConnection getLocalConnection(String url, String username,String password){
		DBConnection c = new DBConnection(url);
		connectionsPool.put(String.valueOf(c.getId()), c);
		Kernel.log(this.getClass(),"Create new local connection "+c.getId() + " -> URL: " + url + " username: " + username + " password: " + password ,Level.INFO);
		return c;
	}
	
	public DBConnection getSystemConnection(String urlSchema){
		return new DBConnection(urlSchema);
	}
	
	public DBConnection getConnection(String connectionID){	
		return connectionsPool.get(connectionID);
	}
	
	public boolean closeConnection(String connectionID) {
		DBConnection connection = connectionsPool.remove(connectionID);
		
		if(connection != null){
			for (ITransaction transaction : connection.getTransactions()) {
				if(!transaction.isFinish()) {
					transaction.abort();					
				}
			}
			Kernel.log(this.getClass(),"Connection " + connectionID + " closed",Level.INFO);
			return true;
		}

		return false;
	}
	
}
