package DBMS.distributed.commitprotocols;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.distributed.ResourceManagerConnection;
import DBMS.distributed.resourceManager.PersistenceMessageService;

public class TwoPhaseProtocol{
	
	
	private ArrayList<ResourceManagerConnection> rs;
	
	public static final String ABORTED = "00";
	public static final String VOTE_REQ = "1a";
	public static final String VOTE_REQ_RESPONSE = "1b";
	public static final String COMMIT = "2a";
	public static final String COMMITED = "2b";
	public static final int COMMIT_PROTOCOL_TIMEOUT = 3000;
	
	public TwoPhaseProtocol(ArrayList<ResourceManagerConnection> rs) {
		this.rs = rs;
	}
	

	@SuppressWarnings("static-access")
	public boolean start() throws UnknownHostException {
		ArrayList<String> responseList = new ArrayList<String>();
		for(ResourceManagerConnection rs : this.rs){
			PersistenceMessageService message = rs.getTransactionManager().getDispatcher().getServer().getPersistenceMessageService();			
					message.insertIntoMessageLog(rs.getAddress().getLocalHost().getHostAddress(), rs.getAddress().getHostAddress(), 
							Integer.parseInt(rs.getRemoteTransaction().getTransactionGlobalID()), VOTE_REQ, "prepare");
					responseList.add(rs.getRemoteTransaction().prepare());
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					isAllPrepared(responseList);
		}
		return true;
	}
	
	
	@SuppressWarnings({"static-access" })
	private synchronized void isAllPrepared(ArrayList<String> responseList) throws UnknownHostException {
		boolean pass = responseList.stream().allMatch(elem -> elem.contains("prepared"));
		
		if(responseList.size() == rs.size() && pass){
		responseList.clear();
		for(ResourceManagerConnection rs : rs){
			PersistenceMessageService message = rs.getTransactionManager().getDispatcher().getServer().getPersistenceMessageService();			
				message.insertIntoMessageLog(rs.getAddress().getLocalHost().getHostAddress(), rs.getAddress().getHostAddress(), 
						Integer.parseInt(rs.getRemoteTransaction().getTransactionGlobalID()), COMMIT, "commit");
				responseList.add(rs.getRemoteTransaction().commit());
				isAllCommited(responseList);
			}
		}else if (responseList.size() == rs.size()){
			abortTransactionBranchs();
		}
	}

	private synchronized void isAllCommited(ArrayList<String> responseList){
		boolean pass = responseList.stream().allMatch(elem -> elem.contains("commited"));
		if (responseList.size() == rs.size() && pass){
			Kernel.log(this.getClass(),"Committed Distributed Transaction",Level.INFO);
		}
	}
	
	
	private void abortTransactionBranchs(){
		for(ResourceManagerConnection rs : rs){
			new Thread(new Runnable() {
				@Override
				public void run() {
					rs.getRemoteTransaction().abort();
				}
			}).start();
		}
	}
	
	
}