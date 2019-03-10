package DBMS.connectionManager.connectionAPI;

import java.io.IOException;
import java.net.InetAddress;

public interface ReciverMessageListener {
	
	
	/**
	 * method that will invoke every time a new message arrives
	 * @param message message received from the device
	 * @param address from the device
	 * @param port from the device
	 * @throws IOException
	 */
	void receiverMessage(String message, InetAddress address, int port) throws IOException;

}
