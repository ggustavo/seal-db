package DBMS.distributed;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import DBMS.Kernel;

public class ExternalTransactionManagerInterface {

	
	private int port;
	private String host;
	
	private List<DistributedContext> contexts;
	
	public ExternalTransactionManagerInterface(String host, int port) {
		contexts = new LinkedList<DistributedContext>();
		this.host = host;
		this.port = port;
		
	
	}

	//private DistributedTransactionManagerController TM = Kernel.getTransactionManager(); 
	
	public DistributedContext getNewContext(String schema) {
		
		DistributedTransactionManagerController TM = Kernel.getTransactionManager(); 
		
		//ResourceManagerConnection nodeA = TM.register(InetAddress.getByName("localhost"), 3000, "company", "admin", "admin");
		DistributedContext context = null;
		try {
			context = new DistributedContext();
			context.setNode(TM.register(InetAddress.getByName(host), port, schema, "admin", "admin"));
			contexts.add(context);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			context = null;
		}
		
		return context;
	
	}
	
	
}
