package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Scanner;

import DBMS.Kernel;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.transactionManager.Transaction;


public class TPCCLoad {
	
	
	public static int count = 0;
	
	
	public static void exec(Transaction transaction, Callback callback) {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

				System.out.println("  <... Load Process  ...>  ");
				
				
				String tpchSourceFile = Kernel.createDirectory("workload") + File.separator;
				
				int total = 0;
				
			
				try {
					
					
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("customer"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("district"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("history"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("item"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("new_order"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("order_line"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("orders"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("stock"), tpchSourceFile);
				loadTable(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("warehouse"), tpchSourceFile);

				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("customer"));
				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("district"));
				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("history"));	
			    total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("item"));
				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("new_order"));
				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("order_line"));
				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("orders"));
				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("stock"));
				total+=showDetails(transaction, Kernel.getCatalog().getSchemabyName("tpcc").getTableByName("warehouse"));
//				
			//	showDetails(null, null);		
				
				System.out.println("\nTotal Number of Tuples: " + total);
				
					
				} catch (FileNotFoundException | InterruptedException | AcquireLockException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				System.out.println("  <... Finish Load ...>");
				
		
				if(callback!=null)callback.call(transaction);
	}
		
	
	public static void loadTable(Transaction transaction, MTable table,String fileName) throws FileNotFoundException, InterruptedException, AcquireLockException {
		fileName = fileName+table.getName()+".csv";
		
		System.out.println("-- Start -- table: " + table + " file: " + fileName);
		
		
		//if(true)return;
		
		File file = new File(fileName);
		Scanner input = new Scanner(file);
		count = 0;
		input.nextLine();
		while (input.hasNextLine()) {
		    String tuple = input.nextLine();
	
		  //  System.out.println(tuple);
		    String [] data = tuple.split(",");
		    String newTuple = "";
		    for (String string : data) {
		    	if(string.contains("ϕ") || string.contains("\\ϕ")) {
		    		System.out.println("DEU RUIM");
		    	}
		    	if(isNumeric(string)) {
		    		newTuple += string + "|";
		    	}else {
		    		newTuple += "'"+string.trim()+"'" + "|";
		    	}
			}
		    
		   //System.out.println(newTuple);
		    //if(true)return;
		    
		    table.writeTuple(transaction, newTuple);
		   // System.gc();
		  //  Thread.sleep(10);
		
		    count++;
		    if(count % 10000 == 0)
		    	System.out.print("..."+count);
   
		}
		System.out.println();
		
		input.close();
		
		System.out.println("-- Finish -- table: " + table + " file: " + fileName + " ---> " + count + " inserted" );
		
	}
	

	private static int showDetails(Transaction transaction, MTable table)  throws FileNotFoundException, InterruptedException {
		if(transaction==null)return 0;
		long lStartTime = System.nanoTime();
		System.out.println("--------------------------------------------------------");
		System.out.println("Table: " + table.getName());
		System.out.println("Tuples: " + table.getNumberOfTuples(transaction));
		long lEndTime = System.nanoTime();
		long output = lEndTime - lStartTime;
	    System.out.println("Table Scan (Elapsed time): " + output / 1000000 + " ms");
		return table.getNumberOfTuples(transaction);
	}
	
	
	private static boolean isNumeric(String a) {
		try {
			Double.parseDouble(a);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	
	public static void createSchema(Transaction transaction) throws AcquireLockException {
		try {
			Plan createDatabase = new Parse().parseSQL("create database tpcc", Kernel.getCatalog().getDefaultSchema());
			createDatabase.setTransaction(transaction);
			createDatabase.execute();
			System.out.println();
			System.out.println("--> "+createDatabase.getOptionalMessage());
			
			String[] sqls = { 
					"CREATE TABLE customer " + 
					"( " + 
					"  c_id numeric, " + 
					"  c_d_id numeric, " + 
					"  c_w_id numeric, " + 
					"  c_first varchar, " + 
					"  c_middle varchar, " + 
					"  c_last varchar, " + 
					"  c_street_1 varchar, " + 
					"  c_street_2 varchar, " + 
					"  c_city varchar, " + 
					"  c_state varchar, " + 
					"  c_zip varchar, " + 
					"  c_phone varchar, " + 
					"  c_since varchar, " + 
					"  c_credit varchar, " + 
					"  c_credit_lim numeric, " + 
					"  c_discount numeric, " + 
					"  c_balance numeric, " + 
					"  c_ytd_payment numeric, " + 
					"  c_payment_cnt numeric, " + 
					"  c_delivery_cnt numeric, " + 
					"  c_data varchar " + 
					");",
					
					 
					"CREATE TABLE district " + 
					"( " + 
					"  d_id numeric, " + 
					"  d_w_id numeric, " + 
					"  d_ytd numeric, " + 
					"  d_tax numeric, " + 
					"  d_next_o_id numeric, " + 
					"  d_name varchar, " + 
					"  d_street_1 varchar, " + 
					"  d_street_2 varchar, " + 
					"  d_city varchar, " + 
					"  d_state varchar, " + 
					"  d_zip varchar " + 
					");",
						
						
					"CREATE TABLE history " + 
					"( " + 
					"  h_c_id numeric, " + 
					"  h_c_d_id numeric, " + 
					"  h_c_w_id numeric, " + 
					"  h_d_id numeric, " + 
					"  h_w_id numeric, " + 
					"  h_date varchar, " + 
					"  h_amount numeric, " + 
					"  h_data varchar " + 
					");",	
					
					"CREATE TABLE item " + 
					"( " + 
					"  i_id numeric, " + 
					"  i_im_id numeric, " + 
					"  i_name varchar, " + 
					"  i_price numeric, " + 
					"  i_data varchar " + 
					"); " + 
					"",
					
					"CREATE TABLE new_order " + 
					"( " + 
					"  no_w_id numeric, " + 
					"  no_d_id numeric, " + 
					"  no_o_id numeric " + 
					");",
					
					"CREATE TABLE order_line " + 
					"( " + 
					"  ol_w_id numeric, " + 
					"  ol_d_id numeric, " + 
					"  ol_o_id numeric, " + 
					"  ol_number numeric, " + 
					"  ol_i_id numeric, " + 
					"  ol_delivery_d varchar, " + 
					"  ol_amount numeric, " + 
					"  ol_supply_w_id numeric, " + 
					"  ol_quantity numeric, " + 
					"  ol_dist_info varchar " + 
					");",
					
					"CREATE TABLE orders " + 
					"( " + 
					"  o_id numeric, " + 
					"  o_w_id numeric, " + 
					"  o_d_id numeric, " + 
					"  o_c_id numeric, " + 
					"  o_carrier_id numeric, " + 
					"  o_ol_cnt numeric, " + 
					"  o_all_local numeric, " + 
					"  o_entry_d varchar " + 
					"); " + 
					"",
					
					"CREATE TABLE stock " + 
					"( " + 
					"  s_i_id numeric, " + 
					"  s_w_id numeric, " + 
					"  s_quantity numeric, " + 
					"  s_dist_01 varchar, " + 
					"  s_dist_02 varchar, " + 
					"  s_dist_03 varchar, " + 
					"  s_dist_04 varchar, " + 
					"  s_dist_05 varchar, " + 
					"  s_dist_06 varchar, " + 
					"  s_dist_07 varchar, " + 
					"  s_dist_08 varchar, " + 
					"  s_dist_09 varchar, " + 
					"  s_dist_10 varchar, " + 
					"  s_ytd numeric, " + 
					"  s_order_cnt numeric, " + 
					"  s_remote_cnt numeric, " + 
					"  s_data varchar " + 
					"   " + 
					");",
					
					"CREATE TABLE warehouse " + 
					"( " + 
					"  w_id numeric, " + 
					"  w_ytd numeric, " + 
					"  w_tax numeric, " + 
					"  w_name varchar, " + 
					"  w_street_1 varchar, " + 
					"  w_street_2 varchar, " + 
					"  w_city varchar, " + 
					"  w_state varchar, " + 
					"  w_zip varchar " + 
					"); "

						
				};
			
			
			for (int i = 0; i < sqls.length; i++) {
				Plan createTable = new Parse().parseSQL(sqls[i], Kernel.getCatalog().getSchemabyName("tpcc"));
				createTable.setTransaction(transaction);
				createTable.execute();
			}
			System.out.println();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * 	public static void loadTable(Transaction transaction, MTable table,String fileName) throws FileNotFoundException, InterruptedException {
		
		System.out.println("-- Start -- table: " + table + " file: " + fileName);
		
		//System.out.println(table.getNumberOfBlocks(transaction));
		//System.out.println(table.getNumberOfTuples(transaction));
		
		//if(true)return;
		
		File file = new File(fileName);
		Scanner input = new Scanner(file);
		int count = 0;
		while (input.hasNextLine()) {
		    String tuple = input.nextLine();
		  //  System.out.println(tuple);
		    
		    String sql = "INSERT INTO " + table.getName() + " (";
			
			for (int i = 0; i < table.getColumnNames().length; i++) {
				sql += table.getColumnNames()[i];
				if (i != table.getColumnNames().length - 1) {
					sql += ", ";
				}
			}
			
			sql+=") VALUES (";
			
			String [] data = tuple.split("\\|");
		    
		    for (String string : data) {
		    	if(isNumeric(string)) {
		    		sql += string;
		    	}else {
		    		sql += "'"+string.trim()+"'";
		    	}
		    	if(string != data[data.length-1]) {
		    		sql+=",";
		    	}
			}
			
			sql+=")";
				
			System.out.println(sql);
			    
		    
		    System.gc();
		    Thread.sleep(100);
		    count++;
		    if(count % 10000 == 0)
		    	System.out.println(count);
		    
		}
		
	
		Kernel.getBufferManager().flush();
		
		System.out.println("-- Finish -- table: " + table + " file: " + fileName + " ---> " + count + " inserted" );
		
	}
	 */
	
	
	
}
