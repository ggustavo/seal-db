package tests;

import java.sql.SQLException;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.OperationsAPI;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;

public class TestSQL {
	
	
	public static void main(String[] args) throws InterruptedException {
		Kernel.start(); 
		
		Thread.sleep(2000);
		
		
		DBConnection connection = Kernel.getTransactionManager().getLocalConnection("tpch", "admin", "admin");
		
		ITransaction transaction = Kernel.getExecuteTransactions().begin(connection);
		
		transaction.execRunnable(new TransactionRunnable() {
			
			@Override
			public void run(ITransaction transaction) {
				
				String sql = "select * from region";
				ISchema tpch = Kernel.getCatalog().getSchemabyName("tpch");
				
				Plan plan = null;
				
				try {
					plan = Parse.getNewInstance().parseSQL(sql, tpch);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				
				if(plan != null) {
					ITable result = plan.execute();
					
					OperationsAPI.showResult(transaction, result);	
				}
				
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
