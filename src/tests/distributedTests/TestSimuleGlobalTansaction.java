package tests.distributedTests;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Random;

import DBMS.Kernel;
import DBMS.distributed.Branch;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.distributed.ResourceManagerConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;



public class TestSimuleGlobalTansaction {
	
	public static void main(String[] args) throws RemoteException, UnknownHostException, SQLException, InterruptedException {
		startLocalNode();
		
		
		execGlobalPlan();
	}
	
	
	public static void startLocalNode(){
		int min = 3001;
		int max = 3500;
		
		Random r = new Random();

		Kernel.PORT = r.nextInt((max - min) + 1) + min;	// If you wanted to choose a specific port
		Kernel.start();
	}
	
	
	
	
public static void execGlobalPlan() throws RemoteException, SQLException, UnknownHostException, InterruptedException {
		Thread.sleep(5000);
	
		String transactionGlobalID = String.valueOf(Kernel.getNewID(Kernel.PROPERTIES_TRASACTION_ID));
	
		//System.out.println(transactionGlobalID);
	
		String F1 = "select * from tables";
	
		DistributedTransactionManagerController TM = Kernel.getTransactionManager(); 
		
		
		//ResourceManagerConnection nodeA = TM.register(InetAddress.getByName("localhost"), 3000, "company", "admin", "admin");
		ResourceManagerConnection nodeA = TM.register(InetAddress.getByName("localhost"), 3000, "seal-db", "admin", "admin");
	
		Thread.sleep(2000);
		
		Branch branchNodeA = nodeA.createBranch(transactionGlobalID);

		
		Thread.sleep(2000);
		
		branchNodeA.begin();	
		ITable tableResultBranchA = branchNodeA.execute(F1);	
		printBranchResut(branchNodeA,tableResultBranchA);
				
		Thread.sleep(2000);
		
		branchNodeA.commit();

		
		
		System.out.println("Finish");
		
		//Kernel.stop();
		
		nodeA.unRegister();
		

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