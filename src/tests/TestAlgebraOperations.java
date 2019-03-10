package tests;

import java.util.Arrays;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.OperationsAPI;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.queryProcessing.ITable;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;


public class TestAlgebraOperations {
	
	public static void main(String[] args) throws InterruptedException {
	
		Kernel.start(); 
	
		Thread.sleep(2000);
		
		DBConnection connection = Kernel.getTransactionManager().getLocalConnection("tpch", "admin", "admin");
		
		ITransaction transaction = Kernel.getExecuteTransactions().begin(connection);
		
		transaction.execRunnable(new TransactionRunnable() {
			
			@Override
			public void run(ITransaction transaction) {
				
				Plan plan = new Plan(transaction);
				
				TableOperation part = OperationsAPI.newTable(plan, "tpch", "part");
				
				System.out.println("Table: " + part.getName());
				System.out.println("Table: " + Arrays.toString(part.getPossiblesColumnNames()));
				System.out.println();
				
				TableOperation partSupp = OperationsAPI.newTable(plan, "tpch", "partsupp");
				
				System.out.println("Table: " + partSupp.getName());
				System.out.println("Table: " + Arrays.toString(partSupp.getPossiblesColumnNames()));
				System.out.println();

				SelectionOperation selection = OperationsAPI.newSelection(plan, new Condition("p_size", ">", "40"));
				selection.setLeft(part);

				JoinOperation join = OperationsAPI.newJoin(plan, new Condition("partkey","==","partkey"));
				join.setLeft(part);
				join.setRight(partSupp);
				
				
				ProjectionOperation projection = OperationsAPI.newProjection(plan,"partkey","p_name","p_type","suppkey");
				projection.setLeft(join);
				
				plan.addOperation(projection);
				ITable result = plan.execute();
				
				OperationsAPI.showResult(transaction, result);
			}
			
			@Override
			public void onFail(ITransaction transaction, Exception e) {
				System.out.println(e.getMessage());
				transaction.abort();
				
			}
		});
		
		Thread.sleep(2000);
		Kernel.stop();
		System.out.println("Finish Test");
		System.exit(0);
	}
	
}
