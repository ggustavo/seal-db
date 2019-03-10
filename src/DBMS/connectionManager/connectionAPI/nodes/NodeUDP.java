package DBMS.connectionManager.connectionAPI.nodes;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

import DBMS.connectionManager.connectionAPI.Node;
import DBMS.connectionManager.connectionAPI.ReciverMessageListener;

public class NodeUDP extends Node{

	
	private DatagramSocket datagramSocket; 
	private int buffer;

	

	public NodeUDP(int port) throws SocketException{
		super.port = port;
		this.buffer = 256;
		datagramSocket = new DatagramSocket(port);
	}
	
	public void send(InetAddress address, int port, String message,  int timeout) throws IOException {

		byte[] byteMsg = message.getBytes();
		DatagramPacket pkg = new DatagramPacket(byteMsg, byteMsg.length, address, port);
		this.datagramSocket.send(pkg);
		
	}
	

	public void startReceiver(final ReciverMessageListener rML) {
		
		if (!isStarted())
			new Thread(new Runnable() {
				public void run() {
					try {
						started = true;
						
						while (!datagramSocket.isClosed() && isStarted()) {
							
							byte[] msg = new byte[buffer];
							DatagramPacket pkg = new DatagramPacket(msg, msg.length);
							datagramSocket.receive(pkg);
							
							if(pkg.getData().length>0)
								rML.receiverMessage(new String(pkg.getData()).trim(), pkg.getAddress(), pkg.getPort());
									
						}
						started = false;
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}).start();
			
	}


	public void close() {
		datagramSocket.close();
	}

	
	
	
	
	
}
