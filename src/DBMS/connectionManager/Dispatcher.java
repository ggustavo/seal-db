package DBMS.connectionManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.logging.Level;

import com.google.gson.Gson;

import DBMS.Kernel;
import DBMS.connectionManager.connectionAPI.Node;
import DBMS.connectionManager.connectionAPI.Protocol;
import DBMS.connectionManager.connectionAPI.ReciverMessageListener;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.distributed.resourceManager.message.MessageHeader;

public class Dispatcher {
	
	private Node node;
	private DispatcherListener listener;
	public static Gson gson;
	private DistributedTransactionManagerController server;
	public static boolean RECEIVER_MESSAGES_DEBUG = true;

	private HashMap<Integer, ResponseMenssageListener<?>> responsePool;
	private static int responseIDs = 0;
	
	public Dispatcher(DistributedTransactionManagerController server){
		this.server = server;
		
		try {
			
			node = Node.build(Kernel.PORT, Protocol.TCP);
			responsePool = new HashMap<>();
			Kernel.log(this.getClass(),"Dispatcher Server Starts port:" + Kernel.PORT,Level.CONFIG);
			
			
		} catch (IOException e) {
			Kernel.exception(this.getClass(),e);
		}
		gson = new Gson();

		startReceiver();
	}
	
	private void startReceiver(){
		
		node.startReceiver(new ReciverMessageListener() {
			
			public void receiverMessage(String message, InetAddress address, int port) throws IOException {
				
				dispacher(message, address);
			}
		});
		
	}
	

	private void dispacher(String message, InetAddress address){
		
		MessageHeader m = null;
		try {
			 m = gson.fromJson(message, MessageHeader.class);
			 m.address = address;
			 if(RECEIVER_MESSAGES_DEBUG)Kernel.log(this.getClass(),"Received: "+message,Level.INFO);
		} catch (Exception e) {		
			Kernel.exception(this.getClass(),e);
			return;
		}
		
		if(listener!=null)listener.showMsg( m.address.getHostAddress()+":"+m.port,getName(), m.Type);
		
		switch (m.Type) {
		
			case MessageHeader.NEW_CONNECTION:
				server.getConnectionService().createConnection(m);
			break;
		
			case MessageHeader.CLOSE_CONNECTION:
				server.getConnectionService().closeConnection(m);
			break;
			
			case MessageHeader.BEGIN_TRANSACTION:
				server.getTransactionService().beginTransaction(m);
			break;
			
			case MessageHeader.COMMIT_TRANSACTION:
				server.getTransactionService().commitTransaction(m);
			break;
			
			case MessageHeader.PREPARE:
				server.getTransactionService().prepareTransaction(m);
			break;
			
			case MessageHeader.ABORT:
				server.getTransactionService().abortTransaction(m);
			break;
			
			case MessageHeader.STATUS:
				server.getTransactionService().statusTransaction(m);
			break;
			
			case MessageHeader.EXECUTE_QUERY_SQL:
				server.getTransactionService().executeQuery(m);
			break;
			case MessageHeader.LOG_TRANSACTION:
				server.getTransactionService().logTransaction(m);
			break;
			case MessageHeader.RESPONSE:
				response(m);
			break;
			
			case MessageHeader.MULTI_RESPONSE:
				multiResponse(m);
			break;
			
			default:
			break;
		}
		
	}

	public static String toJson(Object o){
		return gson.toJson(o);
	}
	
	public static <T> T fromJson(String o, Class<T> c){
		return gson.fromJson(o, c);
	}
	

	public void sendResquest(InetAddress address, int port, Object data,char type, ResponseMenssageListener<?> response,  int timeout) throws SocketTimeoutException{
		try {
			MessageHeader m = new MessageHeader();
			
			m.address = node.getInetAddress();
			m.port = node.getPort();
						
			m.Type = type;
			m.setData(data);
			m.responseCode = getNewId();
			String json = gson.toJson(m);
			responsePool.put(m.responseCode, response);
			
			if(listener!=null)listener.showMsg(getName(),m.address.getHostAddress()+":"+m.port, m.Type);
			
			node.send(address, port, json, timeout);
		} catch (SocketTimeoutException e) {
			throw new SocketTimeoutException();
		} catch (IOException e) {
			throw new SocketTimeoutException();
		} 
	}
	
	
	public void sendResponseRequest(MessageHeader m, Object data, int timeout) throws SocketTimeoutException{
		try {
			m.Type = MessageHeader.RESPONSE;
			
			m.setData(data);
		
			String json = gson.toJson(m);
			
			if(listener!=null)listener.showMsg(getName(),m.address.getHostAddress()+":"+m.port, m.Type);
			
			node.send(m.address, m.port, json, timeout);
		} catch (SocketTimeoutException e) {
			throw new SocketTimeoutException();
		} catch (IOException e) {
			throw new SocketTimeoutException();
		} 
		
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void response( MessageHeader m ){
	
		ResponseMenssageListener r = responsePool.get(m.responseCode);
		if(r!=null){
			
			responsePool.remove(m.responseCode);
					
			if (m.fault!=null) {
				r.onErro(  m.fault );
			}else{
				Object data = m.getData(r.responseDataClass());
				r.onReceiver(data);
			
			}
			
		}else{
			Kernel.log(this.getClass(),"No Register to response: "+m.responseCode,Level.SEVERE);
		}
	}
	
	
	public void sendMultiResponseRequest(MessageHeader m, Object data,  int timeout){
		try {
		
			m.Type = MessageHeader.MULTI_RESPONSE;
			m.setData(data);
		
			String json = gson.toJson(m);
			
			if(listener!=null)listener.showMsg(getName(),m.address.getHostAddress()+":"+m.port, m.Type);
			
			node.send(m.address, m.port, json, timeout);
		} catch (Exception e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void multiResponse( MessageHeader m ){
	
		ResponseMenssageListener r = responsePool.get(m.responseCode);
		if(r!=null){
			
			//responsePool.remove(m.responseCode);
					
			if (m.fault!=null) {
				r.onErro(  m.fault );
			}else{
				Object data = m.getData(r.responseDataClass());
				r.onReceiver(data);
				
			}
			
		}else{
			Kernel.log(this.getClass(),"No Register to response: "+m.responseCode,Level.SEVERE);
		}
	}

	public DistributedTransactionManagerController getServer() {
		return server;
	}
	
	@SuppressWarnings("unlikely-arg-type")
	public void closeListener(ResponseMenssageListener<?> ra){
		responsePool.remove(ra);
	}
	
	private synchronized int getNewId(){
		return responseIDs++;
	}

	public DispatcherListener getListener() {
		return listener;
	}

	public void setListener(DispatcherListener listener) {
		this.listener = listener;
	}
	public String getName() {
		return node.getInetAddress().getHostAddress()+":"+node.getPort();
	}
	
}