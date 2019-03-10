package DBMS.connectionManager.connectionAPI.nodes;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import DBMS.connectionManager.connectionAPI.Node;
import DBMS.connectionManager.connectionAPI.ReciverMessageListener;


public class NodeTCP extends Node{
 
	ServerSocket serverSocket;
	
	public NodeTCP(int port) throws IOException{
		serverSocket = new ServerSocket(port);
	}
	
	

	@Override
	public void send(InetAddress address, int port, String message, int timeout) throws IOException, SocketTimeoutException {
		Socket socket = new Socket();
		//socket.connect(new InetSocketAddress(address, port),timeout);
		socket.connect(new InetSocketAddress(address, port));
		writeMessage(formart(getPort(), message), socket.getOutputStream());
		//writeMessage(message, socket.getOutputStream());
		socket.close();
	}

	
	public void startReceiver(final ReciverMessageListener reciverMessageListener) {
		

		if (!isStarted())
			new Thread(new Runnable() {
				public void run() {
					try {
						started = true;
						while (!serverSocket.isClosed() && isStarted()) {
							
							Socket socket = serverSocket.accept();
							
							String message = readMessage(socket.getInputStream());
							
							String fmessage = deformGetMessage(message);
							int port = deformGetPort(message);
							
							reciverMessageListener.receiverMessage(fmessage, socket.getInetAddress(), port);
							socket.close();
							
						}
						started = false;
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}).start();
		
	
	}

	
	public void close() throws IOException {
		serverSocket.close();
	}

	
	private static void writeMessage(String message,OutputStream outputStream ) throws IOException{
		ObjectOutputStream out = new ObjectOutputStream(outputStream);
		out.flush();
        out.writeObject(message);
        //out.close();
	}
	private static String readMessage(InputStream inputStream) throws IOException, ClassNotFoundException{
		ObjectInputStream in = new ObjectInputStream(inputStream);
		String message = (String) in.readObject();
		//in.close();
		return message;
	}

	/*
	private void startReceiverMessage(final ReciverMessageListener reciverListener, final Socket socket){
		new Thread(new Runnable() {
			public void run() {
				String message;
				try {
					while(socket.isConnected()){
						message = readMessage(socket.getInputStream());
						if(message!=null){
							//reciverListener.receiverMessage(message);		
						}
					}
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
			}
		}).start();

	}
	*/
	
	
	
	

}
