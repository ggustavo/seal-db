package DBMS.connectionManager.connectionAPI;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;

import DBMS.Kernel;
import DBMS.connectionManager.connectionAPI.nodes.NodeTCP;
import DBMS.connectionManager.connectionAPI.nodes.NodeUDP;

public abstract class Node {
	
	protected int port;
	protected InetAddress address;
	protected Protocol protocol;
	protected boolean started = false;
	
	protected Node(){
		
	}
	
	/**
	 * @param port the receiving messages server will be started on this port
	 * @param Protocol indicates the protocol will be used in communication
	 * @return an instance for communication
	 * @throws IOException
	 */
	public static Node build(int port, Protocol Protocol) throws IOException {
		Node node = null;
		switch (Protocol) {
		
		case TCP:	
	
			node = new NodeTCP(port);
			break;
			
		case UDP:
		
			node = new NodeUDP(port);	
			break;
		
		default:
			return null;
		}
		node.port = port;
		node.protocol = Protocol;
		node.address = InetAddress.getLocalHost();
		return node;
	}
	
	
	/**
	 * sends a message to a device
	 * @param address 
	 * @param port 
	 * @param message
	 * @throws IOException
	 */
	public abstract void send(InetAddress address, int port, String message, int timeout) throws IOException, SocketTimeoutException ;
	
	/**
	 * Starts receiving messages
	 * @param reciverMessageListener indicates listener for receiving messages
	 */
	public abstract void startReceiver(final ReciverMessageListener reciverMessageListener);
	
	/**
	 * @return informs the receiving messages is enabled
	 */
	public boolean isStarted() {
		return started;
	}
	/**
	 * ends communication
	 * @throws IOException
	 */
	public abstract void close() throws IOException;

	protected String formart(int port,String string){
		return port+"&"+string;
	}
	protected int deformGetPort(String message){
		try{
			String[] s = message.split("&");	
			int port = Integer.parseInt(s[0]);
			return port;
		}catch(Exception e){
			Kernel.exception(this.getClass(),e);
		}
		return -1;
	}
	
	protected String deformGetMessage(String message){
		try{
			String[] s = message.split("&");	
			
			return s[1];	
		}catch(Exception e){
			Kernel.exception(this.getClass(),e);
		}
		return null;
	}
		
	public int getPort() {
		return port;
	}

	public Protocol getProtocol() {
		return protocol;
	}

	public InetAddress getInetAddress() {
		
		return address;
	}

	public String getAddress() {
		
		return address.getHostAddress();
	}
	
	
}
