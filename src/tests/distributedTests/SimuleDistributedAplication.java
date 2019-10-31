package tests.distributedTests;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;


import DBMS.Kernel;
import DBMS.connectionManager.Dispatcher;
import DBMS.distributed.Branch;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.distributed.ResourceManagerConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;

public class SimuleDistributedAplication {
	
	public static void main(String[] args) throws UnknownHostException {

		new SimuleDistributedAplication().start();

	}
	
	private final static String HELP = "help";
	private final static String EXIT = "exit";
	private final static String CONNECT = "connect:";
	private final static String DISCONNECT = "disconnect";
	private final static String SHOW = "show";
	private final static String EXECUTE = "execute"; 
	private final static String COMMIT = "commit";
	private final static String ABORT = "abort";
	
	public static String[] commands = {EXIT,DISCONNECT,CONNECT,SHOW,EXECUTE,HELP,COMMIT};
	
	
	private List<Branch> branches;
	
	public void start() throws UnknownHostException{
		
		startLocalNode();
		Dispatcher.RECEIVER_MESSAGES_DEBUG = false;
		branches = new LinkedList<>();
		Scanner scan = new Scanner(System.in);
		boolean exe = true;
		DistributedTransactionManagerController TM = Kernel.getTransactionManager(); 
		System.out.println("Type a Command: ");
		while(exe){
			String n = scan.nextLine();
			String c = containsCommand(n);
			if(c == null){
				System.out.println("Invalid Command: " + n);
				continue;
			}
			switch(c){
				
				case EXIT:
					exe=false;
					break;
				case CONNECT:
					try{
						
						String res[] = n.split("\\:")[1].split("\\|");
						
						
						ResourceManagerConnection node = TM.register(InetAddress.getByName(
								res[0].trim()), 
								Integer.parseInt(res[1].trim()), 
								res[2].trim(), 
								res[3].trim(), 
								res[4].trim());
						Branch branch = node.createBranch(null);
						branch.begin();
						branches.add(branch);
						
					}catch (Exception e) {
						System.out.println("Invalid Connect for: " + n);
						e.printStackTrace();
					}
					
					break;
				case DISCONNECT:
					try{
						String id = n.split("\\:")[1];
						boolean b = true;
						for (Branch branch : branches) {
							if(String.valueOf(branch.getId()).equalsIgnoreCase(id.trim())){
								branch.getResourceManagerConnection().unRegister();
								b = false;
								break;
							}
						}
						if(b)System.out.println("Branch " + id + "not found");
					}catch (Exception e) {
						System.out.println("Invalid DISCONNECT for: " + n);
						e.printStackTrace();
					}
					
					break;
				
				case SHOW:
					System.out.println("----------- Branch List ----------- ");
					System.out.println("Id\tStatus");
					for (Branch branch : branches) {
						System.out.println(branch.getId() +"\t" +branch.status());
					}
					System.out.println("----------- ----------- ----------- ");
					break;
					
				case ABORT:
					
					try{
						String id = n.split("\\:")[1];
						boolean b = true;
						for (Branch branch : branches) {
							if(String.valueOf(branch.getId()).equalsIgnoreCase(id.trim())){
								branch.abort();
								b = false;
								break;
							}
						}
						if(b)System.out.println("Branch " + id + "not found");
					}catch (Exception e) {
						System.out.println("Invalid DISCONNECT for: " + n);
						e.printStackTrace();
					}
					
					break;
					
					
				case COMMIT:
					
					try{
						String id = n.split("\\:")[1];
						boolean b = true;
						for (Branch branch : branches) {
							if(String.valueOf(branch.getId()).equalsIgnoreCase(id.trim())){
								branch.commit();
								b = false;
								break;
							}
						}
						if(b)System.out.println("Branch " + id + "not found");
					}catch (Exception e) {
						System.out.println("Invalid DISCONNECT for: " + n);
						e.printStackTrace();
					}
					
					break;
			
				case EXECUTE:
					
					new Thread(new Runnable() {
						
						@Override
						public void run() {
							try{
								String id = n.split("\\:")[1].split("\\|")[0];
								String sql = n.split("\\:")[1].split("\\|")[1];
								boolean b = true;
								for (Branch branch : branches) {
									if(String.valueOf(branch.getId()).equalsIgnoreCase(id.trim())){
										
										ITable table = branch.execute(sql);
										printBranchResut(branch, table);
										b = false;
										break;
									}
								}
								if(b)System.out.println("Branch " + id + "not found");
							}catch (Exception e) {
								System.out.println("Invalid Execute for: " + n);
								e.printStackTrace();
							}
							
						}
					}).start();
					
					break;
				

				case HELP:
					String format = "%-30s %-45s %s%n";
					System.out.println("------------------------------ HELP ------------------------------ ");
					
					System.out.printf(format,"  Description","Syntax","Example");
					System.out.println();
					System.out.printf(format,"- Connect to Node","connect: ip|port|schema|user|password","connect: localhost|3000|company|adm|123"); 
					System.out.printf(format,"- Disconnect from Node","diconnect: branch_id","disconnect: 4");
					System.out.printf(format,"- Execute SQL on Node","execute: branch_id|sql","execute: 2|select * from department");
					System.out.printf(format,"- Exit Aplication","exit","exit");
					System.out.printf(format,"- Show all branch id","show","show");
					System.out.printf(format,"- Commit","commit: branch_id","commit: 1");
					System.out.printf(format,"- Abort","abort: branch_id","abort: 1");
					System.out.println("-------------------------- ------------ -------------------------- ");
					break;
					
			
				default:
					System.out.println("Invalid Command: " + n);
					break;
			}
			
			
		}
		scan.close();
		Kernel.stop(); //Safe Exit
		System.exit(0);
	}
	
	public void startLocalNode(){
		Kernel.PORT = (int)(Math.random()*5000);	// If you wanted to choose a specific port
		Kernel.start();
	}
	
	
	public static String containsCommand(String c){
		for (int i = 0; i < commands.length; i++) {
			if(c.toLowerCase().contains(commands[i])){
				return commands[i];
			}
		}
		return null;
	}
	

	public static void printBranchResut(Branch branch, ITable table) {
		System.out.println("****************************************************");
		System.out.println("Show Result for branch: " + branch.getId());
		System.out.println("****************************************************");
		if (table != null) {

			TableScan scan = new TableScan(branch.getLocalTransaction(), table);
			ITuple tuple = null;

			while ((tuple = scan.nextTuple()) != null) {
				System.out.println(tuple.getStringData());
			}

		}
		System.out.println("****************************************************");
		System.out.println("Ends Result for branch: " + branch.getId());
		System.out.println("****************************************************");

	}
}
