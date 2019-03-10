package DBMS.distributed.commitprotocols;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.distributed.ResourceManagerConnection;

public abstract class CommitProtocol {
	
	private ArrayList<ResourceManagerConnection> rs;
    
	protected ExecutorService executorService;
	
	public static final int COMMIT_PROTOCOL_TIMEOUT = 3000;
	
	public CommitProtocol(ArrayList<ResourceManagerConnection> rs) {
		this.rs = rs;
		this.executorService = Executors.newFixedThreadPool(this.rs.size());
	}
	
	public abstract boolean start() throws UnknownHostException;
	
	public void abortTransactionBranchs(){
		Kernel.log(this.getClass(),"Send Abort Msg", Level.WARNING);
		for(ResourceManagerConnection rs : this.getResourceManagers()){
			new Thread(new Runnable() {
				@Override
				public void run() {
					rs.getRemoteTransaction().abort();
				}
			}).start();
		}
	}
	
	public ArrayList<ResourceManagerConnection> getResourceManagers() {
		return rs;
	}
}