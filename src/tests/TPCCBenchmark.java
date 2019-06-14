package tests;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;

import DBMS.Kernel;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.recoveryManager.RedoLog;
import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.TransactionRunnable;


public class TPCCBenchmark implements Callback{
	
	public int numberOfTransactions;
	public boolean serial;
	public int tIds = 1;
	
	public static boolean debug = true;

	long lStartTime;
	
	public void startBenchmark(){
			
		System.out.println("\n\n <... Starting TPCC Benchmark ...>");
		System.out.println("Number of Transactions: " + numberOfTransactions);
		System.out.println("Serial: " + serial);
		System.out.println();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		
			e.printStackTrace();
		}
	
		lStartTime = System.nanoTime();
		
		if(!serial) {
			for (int i = 0; i < numberOfTransactions; i++) {
				executeTransaction(this);
			}
		}else {
			executeTransaction(this);
		}
	
	}
	
	private int countExecutions = 0;
	
	@Override
	public synchronized Transaction call(Transaction t) {
		countExecutions++;
		if(countExecutions == numberOfTransactions) {
			System.out.println("\n\n <... Finish TPCC Benchmark  ...> total time: " + (System.nanoTime() - lStartTime) / 1000000 + " ms");
			if(Kernel.ENABLE_HOT_COLD_DATA_ALGORITHMS) {
				System.out.println("Use: " +Kernel.getMemoryAcessManager().getAlgorithm().getClass().getSimpleName());
				System.out.println(Kernel.getMemoryAcessManager().getAlgorithm().showStatics());
				Kernel.getMemoryAcessManager().getAlgorithm().saveData();
				Kernel.getMemoryAcessManager().getAlgorithm().saveCold();
			}
			if(Kernel.ENABLE_RECOVERY)RedoLog.saveLogEvents();
			Kernel.getRecoveryManager().forceFlush();
			
			//System.exit(0);
			return null;
		}
		if(serial==true) {
			executeTransaction(this);
		}
		return null;
	}

	
	
	public void executeTransaction(Callback call) {
		
		Kernel.getExecuteTransactions().execute(new TransactionRunnable() {
			@Override
			public void run(Transaction transaction) throws AcquireLockException {
				if(serial==false)((Transaction)transaction).setIdT(tIds++);
				
				long lStartTime = System.nanoTime();
				boolean all = false;
				
				//int a = gen.nextInt(2);	
				
				int a = transaction.getIdT() % 5;				
			
				if(all||a==0)eQ1(transaction);
				
				if(all||a==1)eQ2(transaction);
				
				//-----------------------------
				
				if(all||a==2)eQ4(transaction);
				
				
				
				if(all||a==3)eQ5(transaction);
			
				if(all||a==4)eQ3(transaction);
				
				
//				Schema s = Kernel.getCatalog().getSchemabyName("tpcc");
//				MTable m = s.getTableByName("customer");
//				
//				
//				for (int i = 0; i < 100; i++) {
//					String id = "" + (gen.nextInt(m.getTuplesHash().size()-100) + 1);
//					String data[] = m.getTuple(transaction, id).getData();
//					m.updateTuple(transaction, data, id);					
//					
//				}
				
			
				transaction.commit();

				RedoLog.EVENTS.add("C," + transaction.getIdT() + "," + lStartTime + "," + System.nanoTime() );
				
				transaction.setState(Transaction.COMMITTED);
				//if(debug)System.out.println("\n <... Finish Transaction T"+transaction.getIdT()+" ...> total time: " + (System.nanoTime() - lStartTime) / 1000000 + " ms");
				if(call!=null)call.call(transaction);
			}
			
			@Override
			public void onFail(Transaction transaction, Exception e) {
				if(debug)
					System.out.println("ABORT ("+transaction.getIdT()+")-> "+e.getMessage());
			//	e.printStackTrace();
				transaction.abort();
				if(call!=null)call.call(transaction);
				//System.exit(0);
			}
		},true,true);
	}
	
	private static Random gen = new Random();

	
	public static String getRandom(String... values) {
		
		return values[gen.nextInt(values.length)];
	}
	
	
	public static String values[] = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };
	public String [] warehouseList = {"1"};
	
	
	public static String getLastName(int value) {
		value = Math.min(999, value);
		return "'"+(values[(int)(value/100)] +  values[(int)((value/10)%10)] +  values[(int)(value%10)]+"'");
	}
	
	
	public void eQ1(Transaction transaction) throws AcquireLockException {
		String w_id = getRandom(warehouseList);
	//	String d_id = getRandom("4","5","9","1","8","2","6","3","7","10");
		String d_id = "1";
		String c_id = ""+(gen.nextInt(3000) + 1);
		String o_ol_cnt = getRandom("11","8","10","5","15","6","9","7","12","13","14"); 
		String o_all_local = "1";		
		
//		String w_id = "1";
//		String d_id = "5";
//		String c_id = "1000";
//		String o_ol_cnt = "14"; 
//		String o_all_local = "1";	
//		
		calcTime(() -> Q1(transaction, w_id, d_id, c_id, o_ol_cnt, o_all_local ),"T"+transaction.getIdT()+"-Q1");
	}
	
	public void eQ2(Transaction transaction) throws AcquireLockException {
		
		String w_id = getRandom(warehouseList);
		String h_amount = "10";
		String d_id = getRandom("4","5","9","1","8","2","6","3","7","10");
		String c_last = ""+(gen.nextInt(1000) + 1); 
		String c_d_id = getRandom("4","5","9","1","8","2","6","3","7","10");
		String c_w_id = w_id;
		String c_id = ""+(gen.nextInt(3000) + 1);
		boolean byname = gen.nextBoolean();
		
//		String w_id = "1";
//		String h_amount = "10";
//		String d_id = "5";
//		String c_last = "599";
//		String c_d_id = "9";
//		String c_w_id = w_id;
//		String c_id = "2400";
//		boolean byname = true;
////		
		calcTime(() -> Q2(transaction,w_id, h_amount, d_id, c_last, c_d_id, c_w_id, c_id, byname),"T"+transaction.getIdT()+"-Q2");
	}
	
	public void eQ3(Transaction transaction) throws AcquireLockException {
		String w_id = getRandom(warehouseList);
		String d_id = getRandom("4","5","9","1","8","2","6","3","7","10");
		String c_id = ""+(gen.nextInt(3000) + 1);
		String c_last = ""+(gen.nextInt(1000) + 1); 
		boolean byname = gen.nextBoolean();
		
//		String w_id = "1";
//		String d_id = "4";
//		String c_id = "2300";
//		String c_last = "559";
//		boolean byname = true;
		
		calcTime(() -> Q3(transaction,w_id, d_id, c_id,c_last, byname),"T"+transaction.getIdT()+"-Q3");	
	}
	
	public void eQ4(Transaction transaction) throws AcquireLockException {
		
		String w_id = getRandom(warehouseList);
		String o_carrier_id = getRandom("4", "5", "NULL", "9","1","8","2","6","3","7","10");
		
		//String w_id = "1"; 
		//String o_carrier_id ="8";
		
		calcTime(() -> Q4(transaction, w_id, o_carrier_id),"T"+transaction.getIdT()+"-Q4");
	}
	
	public void eQ5(Transaction transaction) throws AcquireLockException {

		String w_id = getRandom(warehouseList);
		String d_id = getRandom("4","5","9","1","8","2","6","3","7","10");
		
//		String w_id = "1";
//		String d_id = "9";
		
		calcTime(() -> Q5(transaction, w_id, d_id),"T"+transaction.getIdT()+"-Q5");

	}
	
	
	
	
	public static void Q1_v22(Transaction transaction, String w_id, String d_id, String c_id, String o_ol_cnt, String o_all_local ) throws AcquireLockException { 
		
			String sql = " UPDATE district SET d_next_o_id = "+ ((d_id) + 1) +
					 "  WHERE d_id = " + d_id+ " AND d_w_id = " + w_id;
			
			execSQL(sql, transaction);

	/*
		sql =  " INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,o_entry_d, o_ol_cnt, o_all_local) "+
			   " VALUES ("+((d_id) + 1) +", "+d_id+", "+w_id+", "+c_id+", "+ "'Date2019'" +", "+o_ol_cnt+", "+o_all_local+");";
		
		execSQL(sql, transaction);
		
		sql =  "insert into -new_order- (no_o_id, no_d_id, no_w_id) VALUES ("+((d_id) + 1)+", "+d_id+", "+d_id+");";
		sql = sql.replaceAll("-", " ");
		
		execSQL(sql, transaction);
		*/
			
	}
	
	public static void Q2_v22(Transaction transaction, String w_id, String h_amount, String d_id, String c_last, String c_d_id, String c_w_id, String c_id, boolean byname) throws AcquireLockException {
		c_last = getLastName(Integer.parseInt(c_last));
		

			
		String sql =  "UPDATE warehouse SET w_ytd = " + h_amount+ //w_ytd = w_ytd + h_amount
					  "  WHERE w_id= "+w_id+";"; 
		
		execSQL(sql, transaction);
		
		sql =  "UPDATE  district  SET d_ytd  = " + h_amount + //d_ytd  = d_ytd  + h_amount
				" WHERE d_w_id= " + w_id + " AND d_id="+d_id; 
	
		execSQL(sql, transaction);
		
		String c_balance = "-10";
		String c_new_data = "FFF040E0ABB1";
		
		
		sql = " UPDATE customer "+
				 " SET c_balance = " +c_balance+", c_data = "+c_new_data +
				 "  WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND "+
				 " c_id = "+c_id+"; "; 
				
				execSQL(sql, transaction);


		sql = " UPDATE customer SET c_balance = "+ c_balance +
	 					"  WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND "+
	 					" c_id = " + c_id+ ";"; 
					 
					execSQL(sql, transaction);

/*
		String h_data = "FFF040E0ABB1355ACCE";
					
		sql = " INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, " +
						" h_w_id, h_date, h_amount, h_data) " +
						" VALUES ("+c_d_id+", "+c_w_id+", "+c_id+", "+d_id+", " +
						w_id+" , "+"'Date2019'"+", " + h_amount + ", "+h_data+");  ";
				
				execSQL(sql, transaction);
		*/
	}
	
	
	
	
	
	public static void Q1(Transaction transaction, String w_id, String d_id, String c_id, String o_ol_cnt, String o_all_local ) throws AcquireLockException { 
		
		
		String datetime = "'Date2019'";

		
		String sql = " SELECT c_discount, c_last, c_credit, w_tax " +
		// " INTO :c_discount, :c_last, :c_credit, :w_tax
		" FROM customer, warehouse " +
		"  WHERE w_id = " + w_id + " AND c_w_id = w_id AND " + 
		" c_d_id = " + d_id + " AND c_id = " + c_id + "; ";
	
		IntoVariable c_discount = new IntoVariable();
		IntoVariable c_last = new IntoVariable();
		IntoVariable c_credit = new IntoVariable();
		IntoVariable w_tax = new IntoVariable();
		
		if(execAndIntoSQL(sql,transaction,c_discount,c_last,c_credit, w_tax) == null) return;
	
//		if(debug)System.out.println(c_discount);
//		if(debug)System.out.println(c_last);
//		if(debug)System.out.println(c_credit);
//		if(debug)System.out.println(w_tax);
		
		IntoVariable d_next_o_id = new IntoVariable();
		IntoVariable d_tax = new IntoVariable();
		
		sql = " SELECT d_next_o_id, d_tax " +
			// "INTO :d_next_o_id, :d_tax "+
			" FROM district "+
			"  WHERE d_id = "+d_id+" AND d_w_id = "+w_id+";";
				
		if(execAndIntoSQL(sql, transaction, d_next_o_id,d_tax) == null) return;
		
//		if(debug)System.out.println(d_next_o_id);
//		if(debug)System.out.println(d_tax);
		
		

		sql = " UPDATE district SET d_next_o_id = "+ (new Integer(d_next_o_id.v) + 1) +
				 "  WHERE d_id = " + d_id+ " AND d_w_id = " + w_id;
		
		execSQL(sql, transaction);
		
		String  o_id = d_next_o_id.v;
	
		 
		
		sql =  " INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,o_entry_d, o_ol_cnt, o_all_local) "+
			   " VALUES ("+o_id +", "+d_id+", "+w_id+", "+c_id+", "+ datetime +", "+o_ol_cnt+", "+o_all_local+");";
		
		
		execSQL(sql, transaction);
		
		sql =  "insert into -new_order- (no_o_id, no_d_id, no_w_id) VALUES ("+o_id+", "+d_id+", "+d_id+");";
		sql = sql.replaceAll("-", " ");
		execSQL(sql, transaction);
		
		
		
		String supware[] = {"1","2","6"}; 
		String itemid[] = {"1","2","6"};  
		String qty[] = {"1","2","6"};
		
		String iname[] = new String[new Integer(o_ol_cnt)];
		String price[] = new String[new Integer(o_ol_cnt)];
		String stock[] = new String[new Integer(o_ol_cnt)];
		
		char bg[] = new char[new Integer(o_ol_cnt)];
		
		double amt[] = new double[new Integer(o_ol_cnt)];
		@SuppressWarnings("unused")
		double total = 0;
		
		for (int ol_number = 1; ol_number <= new Integer(o_ol_cnt); ol_number++) {
			
			int ol_supply_w_id = new Integer(supware[ol_number-1]); //=atol(supware[ol_number-1])
			if (ol_supply_w_id != new Integer(w_id)) o_all_local = "0"; 
			int ol_i_id = new Integer(itemid[ol_number-1]);
			int ol_quantity = new Integer(qty[ol_number-1]); 
			
			//EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; ????
			
			IntoVariable i_price = new IntoVariable();
			IntoVariable i_name = new IntoVariable();
			IntoVariable i_data = new IntoVariable();
			
			sql = " SELECT i_price, i_name , i_data "+
					// INTO :i_price, :i_name, :i_data
					 " FROM item "+ 
					 "  WHERE i_id = "+ol_i_id; 
			
			if(execAndIntoSQL(sql, transaction, i_price, i_name, i_data) == null) continue;
			
//			if(debug)System.out.println(i_price);
//			if(debug)System.out.println(i_name);
//			if(debug)System.out.println(i_data);
						
			 price[ol_number-1] = i_price.v; 
			 iname[ol_number-1] = i_name.v; //strncpy(iname[ol_number-1],i_name,24);
			 
			 //EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; 
			 
			 IntoVariable s_quantity = new IntoVariable();
			 IntoVariable s_data = new IntoVariable();
			 IntoVariable s_dist_01 = new IntoVariable();
			 IntoVariable s_dist_02 = new IntoVariable();
			 IntoVariable s_dist_03 = new IntoVariable();
			 IntoVariable s_dist_04 = new IntoVariable();
			 IntoVariable s_dist_05 = new IntoVariable();
			// Param s_dist_06 = new Param(); Sem dist06
			 IntoVariable s_dist_07 = new IntoVariable();
			 IntoVariable s_dist_08 = new IntoVariable();
			 IntoVariable s_dist_09 = new IntoVariable();
			 IntoVariable s_dist_10 = new IntoVariable();
			 
			
			 sql = " SELECT s_quantity, s_data, "+
					 " s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
					// " s_dist_06, "
					 + " s_dist_07, s_dist_08, s_dist_09, s_dist_10 "+
	
					 " FROM stock "+
					 "  WHERE s_i_id = "+ol_i_id+" AND s_w_id = "+ol_supply_w_id +";";
			 
			 if(execAndIntoSQL(sql, transaction, 
					 s_quantity,
					 s_data,
					 s_dist_01,
					 s_dist_02,
					 s_dist_03,
					 s_dist_04,
					 s_dist_05,
				//	 s_dist_06,
					 s_dist_07,
					 s_dist_08,
					 s_dist_09,
					 s_dist_10) == null) return;
			 
			
				
			 double ol_dist_info = 0;
			 //pick_dist_info(ol_dist_info, ol_w_id); //pick correct s_dist_xx
			 stock[ol_number-1] = s_quantity.v; 
			 
			
			 
			if ((strstr(i_data.v, "original") > 0) && (strstr(s_data.v, "original") > 0)) {
				bg[ol_number - 1] = 'B';
			} else {
				bg[ol_number - 1] = 'G';

			}
			
			 if (new Integer(s_quantity.v) > ol_quantity) {
				 s_quantity.v = new Integer(s_quantity.v) - ol_quantity + "";	 
			 }else{
				 s_quantity.v = (new Integer(s_quantity.v) - ol_quantity + 91)+"";				 
			 }
			 
			 
			 sql = " UPDATE stock SET s_quantity = "+ s_quantity +
					 "   WHERE s_i_id = "+ ol_i_id +
					 " AND s_w_id = "+ol_supply_w_id + ";"; 
			 
			 execSQL(sql, transaction);
			 
			 double ol_amount = new Integer(ol_quantity) * 
					 new Double(i_price.v) * 
					 (1+ new Double(w_tax.v) + new Double(d_tax.v)) * 
					 (1 - new Double(c_discount.v));
			 
			 amt[ol_number-1] = ol_amount;
			 total += ol_amount; 
			
			 
			 sql = 
			" INSERT "+
			"  INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, "+
			" ol_quantity, ol_amount, ol_dist_info) "+
			"  VALUES ("+ o_id +" , " + d_id + " , " + w_id + " , " + ol_number + " , " +
			"  " + ol_i_id + " , "+ ol_supply_w_id + " , " +
			"  " + ol_quantity + " , " + ol_amount + " , " + ol_dist_info + "); ";
			 
			execSQL(sql, transaction);
		}
		
		//EXEC SQL COMMIT WORK; 
		
	}

	public static void Q2(Transaction transaction, String w_id, String h_amount, String d_id, String c_last, String c_d_id, String c_w_id, String c_id, boolean byname) throws AcquireLockException {
		c_last = getLastName(Integer.parseInt(c_last));
		
		String datetime = "'Date2019'";
			
		
		String sql =  "UPDATE warehouse SET w_ytd = " + h_amount+ //w_ytd = w_ytd + h_amount
					  "  WHERE w_id= "+w_id+";"; 
		
		execSQL(sql, transaction);
		
	
		sql = " SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "+
		 //INTO :w _street_1, :w_street_2, :w_city, :w _state, :w_zip, :w_name
		 " FROM warehouse "+
		 "   WHERE w_id ="+ w_id;
		
		IntoVariable w_street_1 = new IntoVariable();
		IntoVariable w_street_2 = new IntoVariable();
		IntoVariable w_city = new IntoVariable();
		IntoVariable w_state = new IntoVariable();
		IntoVariable w_zip = new IntoVariable();
		IntoVariable w_name = new IntoVariable();
		
		if(execAndIntoSQL(sql, transaction, w_street_1,w_street_2,w_city,w_state,w_zip,w_name) == null) return;
		
		
	
		
		sql =  "UPDATE  district  SET d_ytd  = " + h_amount + //d_ytd  = d_ytd  + h_amount
				" WHERE d_w_id= " + w_id + " AND d_id="+d_id; 
	
		execSQL(sql, transaction);
		
		IntoVariable d_street_1 = new IntoVariable();
		IntoVariable d_street_2 =new IntoVariable();
		IntoVariable d_city = new IntoVariable();
		IntoVariable d_state = new IntoVariable();
		IntoVariable d_zip = new IntoVariable();
		IntoVariable d_name = new IntoVariable();
		
		sql = "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name "+
		// INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		 " FROM district " +
		 "  WHERE d_w_id="+w_id+" AND d_id = "+d_id+"; ";
		
		if(execAndIntoSQL(sql, transaction, d_street_1, d_street_2,d_city,d_state,d_zip,d_name) == null) return;
		
		
		//String byname = ""; //param?
		
		IntoVariable namecnt = new IntoVariable();

		
		if(byname) {
		
			sql = "SELECT count(c_id) "+
					" FROM customer " +
					" WHERE c_last= "+c_last+" AND c_d_id= "+c_d_id+" AND c_w_id= " + c_w_id; 
			
			if(execAndIntoSQL(sql,transaction,namecnt) == null) return;
			//CURSOR
			sql = "SELECT * "+
					" FROM customer " +
					" WHERE c_last= "+c_last+" AND c_d_id= "+c_d_id+" AND c_w_id= " + c_w_id; 
			
			MTable result = execSQL(sql, transaction);
			
			
			if (Integer.parseInt(namecnt.v) % 2 == 0) {
				namecnt.v = Integer.parseInt(namecnt.v)+1+""; //Locate midpoint customer
			}
	
		
			TableScan cursor = new TableScan(transaction, result);
			Tuple tuple = cursor.nextTuple();
			
			for (int n=0; n<Integer.parseInt(namecnt.v)/2  && tuple!=null; n++){ 
				//if(debug)System.out.println(Arrays.toString(tuple.getData()));
				tuple = cursor.nextTuple();
			}
			
			
			
		}else {
			
			sql = " SELECT c_first, c_middle, c_last, " +
					" c_street_1, c_street_2, c_city, c_state, c_zip, "+
					"  c_phone, c_credit, c_credit_lim, "+
					"  c_discount, c_balance, c_since "+
					" FROM customer " +
					" WHERE c_last= "+c_last+" AND c_d_id= "+c_d_id+" AND c_w_id= " + c_w_id; 
			
			
				IntoVariable c_first = new IntoVariable();
				IntoVariable c_middle = new IntoVariable();
				IntoVariable c_last_2 = new IntoVariable();
				IntoVariable c_street_1 = new IntoVariable();
				IntoVariable c_street_2 = new IntoVariable();
				IntoVariable c_city = new IntoVariable();
				IntoVariable c_state = new IntoVariable();
				IntoVariable c_zip = new IntoVariable();
				IntoVariable c_phone = new IntoVariable();
				IntoVariable c_credit = new IntoVariable();
				IntoVariable c_credit_lim = new IntoVariable();
				IntoVariable c_discount = new IntoVariable();
				IntoVariable c_balance = new IntoVariable();
				IntoVariable c_since = new IntoVariable();
				
				if(execAndIntoSQL(sql, transaction, c_first, c_middle, c_last_2,
						 c_street_1, c_street_2, c_city, c_state, c_zip,
						 c_phone, c_credit, c_credit_lim,
						 c_discount, c_balance, c_since ) == null) return;
				
				c_balance.v = Double.parseDouble(c_balance.v) + Double.parseDouble(h_amount) + "";
				//c_credit[2]=' \0'; 
				
				
				if (strstr(c_credit.v, "BC") == 0 ){
				
				IntoVariable c_data = new IntoVariable();
				
				
				
				sql = "  SELECT c_data " +
						//+ "INTO :c_data " +
					" FROM customer " +
					"  WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND c_id = "+c_id+";";
				
				if(execAndIntoSQL(sql,transaction,c_data) == null) return;
				
				/*
				sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f %12c %24c",
	 			c_id,c_d_id,c_w_id,d_id,w_id,h_amount,
	 			h_date, h_data);
	 			strncat(c_new_data,c_data,500-strlen(c_new_data)); 
				 */
				
				String c_new_data = c_data +"h"+ c_id +"h"+c_d_id +"h"+c_w_id +"h"+d_id +"h"+ w_id +"h"+h_amount +"h";
				
				sql = " UPDATE customer "+
				 " SET c_balance = " +c_balance+", c_data = "+c_new_data +
				 "  WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND "+
				 " c_id = "+c_id+"; "; 
				
				execSQL(sql, transaction);
					
				}else {
	
					sql = " UPDATE customer SET c_balance = "+ c_balance +
	 					"  WHERE c_w_id = "+c_w_id+" AND c_d_id = "+c_d_id+" AND "+
	 					" c_id = " + c_id+ ";"; 
					 
					execSQL(sql, transaction);
					
				}
				
				//String h_data = w_name.v + " "+ d_name.v;
				String h_data = "'"+(w_name.v + d_name.v).replaceAll("'","")+"'";
				
				sql = " INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, " +
						" h_w_id, h_date, h_amount, h_data) " +
						" VALUES ("+c_d_id+", "+c_w_id+", "+c_id+", "+d_id+", " +
						w_id+" , "+datetime+", " + h_amount + ", "+h_data+");  ";
				
				execSQL(sql, transaction);
			
		}
		
		
		
		
		
	}
			
	
	public static void Q3(Transaction transaction, String w_id, String d_id, String c_id, String c_last, boolean byname) throws AcquireLockException {
	
		String sql;

		if (byname){
			

			IntoVariable namecnt = new IntoVariable();
			
			sql = " SELECT count(c_id) " +
					//INTO :namecnt
					" FROM customer "+
					"  WHERE c_last="+c_last+" AND c_d_id= "+d_id+" AND c_w_id= " + w_id + ";";
			
			if(execAndIntoSQL(sql, transaction, namecnt) == null)return;
			
			sql = " SELECT c_balance, c_first, c_middle, c_id "+
			 " FROM customer "+
			 "  WHERE c_last = "+c_last+" AND c_d_id ="+d_id+" AND c_w_id = "+w_id+ " "
			+ " ORDER BY c_first; ";
		
			MTable result = execSQL(sql, transaction);
	
			TableScan c_name = new TableScan(transaction, result);
			Tuple tuple = c_name.nextTuple();
	
			if (Double.parseDouble(namecnt.v) % 2 == 0) namecnt.v = (Double.parseDouble(namecnt.v) + 1 ) +"";  // Locate midpoint customer
			
			for (int n=0; n<Double.parseDouble(namecnt.v)/ 2 && tuple!=null; n++){
			 
				/*
				 * EXEC SQL FETCH c_name
	 				INTO :c_balance, :c_first, :c_middle, :c_id; 
				 */
				
				tuple =  c_name.nextTuple();
			}
	
		}else {
			IntoVariable c_balance = new IntoVariable();
			IntoVariable c_first = new IntoVariable();
			IntoVariable c_middle = new IntoVariable();
			IntoVariable c_last_INTO = new IntoVariable();

			sql = " SELECT c_balance, c_first, c_middle, c_last "+
			 //" INTO :c_balance, :c_first, :c_middle, :c_last "+
			 " FROM customer "+
			 "  WHERE c_id = "+c_id+" AND c_d_id = "+d_id+" AND c_w_id = "+w_id+";  ";
			
			if(execAndIntoSQL(sql, transaction, c_balance,c_first,c_middle,c_last_INTO) == null)return;;
			
		}
		
		
		IntoVariable o_id = new IntoVariable(); 
		IntoVariable o_carrier_id = new IntoVariable();
		IntoVariable entdate = new IntoVariable();
	
		
		sql = " SELECT o_id, o_carrier_id, o_entry_d"+
		 // INTO :o_id, :o_carrier_id, :entdate
		 " FROM orders "
		+ " ORDER BY o_id DESC;"; 
		
		if(execAndIntoSQL(sql, transaction, o_id,o_carrier_id,entdate)== null)return;;
		
		sql = " SELECT ol_i_id, ol_supply_w_id, ol_quantity, "+
		 " ol_amount, ol_delivery_d "+
		 " FROM order_line "+
		 "  WHERE ol_o_id="+o_id+" AND ol_d_id="+d_id+" AND ol_w_id="+w_id+";";
		
		MTable result = execSQL(sql, transaction);
	
		TableScan  c_line = new TableScan(transaction, result);
		Tuple tuple =  c_line.nextTuple();
		
		while(tuple!=null) {
			/*
			  	EXEC SQL FETCH c_line
	 			INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i],
	 			:ol_amount[i], :ol_delivery_d[i]; 
			 */
			
			tuple = c_line.nextTuple();
		}
			
	}
	
	public static void Q4(Transaction transaction, String w_id, String o_carrier_id) throws AcquireLockException {
	
		String datetime = "'2018-10-20 21:31:57'";
	
		int DIST_PER_WARE = 10;
		
		
		 for (int d_id=1; d_id <= DIST_PER_WARE; d_id++){ 
		
			 
			IntoVariable no_o_id = new IntoVariable();
			
			String sql = "SELECT no_o_id " +
			" FROM new_order " +
			"  WHERE no_d_id = "+d_id+" AND no_w_id = "+w_id+" " +
			" ORDER BY no_o_id; ";
			
			//EXEC SQL DELETE FROM new_order  WHERE CURRENT OF c_no; 
			
			if(execAndIntoSQL(sql, transaction,no_o_id) == null)return;
			
			sql = " SELECT o_c_id "+
					//INTO :c_id 
					" FROM orders "+
					"  WHERE o_id = "+no_o_id+" AND o_d_id = "+d_id+" AND "+
					" o_w_id = "+w_id+"; ";
			
		
			IntoVariable c_id = new IntoVariable();
			
			if(execAndIntoSQL(sql, transaction, c_id) == null)return;
		
			sql =  " UPDATE orders SET o_carrier_id = "+ o_carrier_id +
					"  WHERE o_id = "+no_o_id+" AND o_d_id = "+d_id+" AND "+
					" o_w_id = "+w_id+"; ";
			
			execSQL(sql, transaction);
			
			sql =  "UPDATE order_line SET ol_delivery_d = " + datetime +
					"  WHERE ol_o_id = "+no_o_id+" AND ol_d_id = "+d_id+" AND "+
					" ol_w_id = "+w_id+"; ";
			
			execSQL(sql, transaction);
		
			IntoVariable ol_total = new IntoVariable(); 
			
			sql = "SELECT SUM(ol_amount) "+
					 //INTO :ol_total
					 " FROM order_line "+
					 "  WHERE ol_o_id = "+no_o_id+" AND ol_d_id = "+d_id+" AND "+
					 " ol_w_id = "+w_id+"; ";
			
			if(execAndIntoSQL(sql, transaction, ol_total) == null)return;
			
			sql =  "UPDATE customer SET c_balance = "+ ol_total + //c_balance = c_balance + ol_total 
					"  WHERE c_id = "+no_o_id+" AND  c_d_id  = "+d_id+" AND "+
					 " c_w_id = "+w_id+"; ";
			
			execSQL(sql, transaction);
			 
		 }
		
	}
	
		
	public static void Q5(Transaction transaction, String w_id, String d_id) throws AcquireLockException { //slev() 
		
	
	String sql = "Select d_next_o_id "+
				//INTO :o_id
				" FROM district "+
				"  WHERE d_w_id="+w_id+" AND d_id="+d_id+"; ";
	
	IntoVariable o_id = new IntoVariable();
	
	if(execAndIntoSQL(sql, transaction, o_id) == null)return;
	
	
	String threshold = "20";
	
	sql =  " SELECT COUNT(s_i_id) "+ //SELECT COUNT(DISTINCT (s_i_id)) 
			//"INTO :stock_count "+
			" FROM order_line, stock "+
			"  WHERE ol_w_id= "+ w_id +" AND "+
			" ol_d_id= " + d_id +" AND ol_o_id < "+ o_id.v +" AND "+
			" ol_o_id >= "+(new Integer(o_id.v)-20)+" AND s_w_id = "+ w_id +" AND "+
			" s_i_id = ol_i_id AND s_quantity < "+ threshold +"; ";
		
	
	IntoVariable stock_count = new IntoVariable();
	
	if(execAndIntoSQL(sql, transaction, stock_count) == null)return;
	
	
	}
	

	
	public static String getFirstResult(Transaction transaction, MTable result) throws AcquireLockException {
		Tuple t = new TableScan(transaction, result).nextTuple();
		if (t != null) {
			return t.getStringData();
		} else {
			return null;
		}

	}

	public static void showResult(Transaction transaction, MTable result, int lines) throws AcquireLockException {
		if(debug)if(debug)System.out.println("\n---------------------------------------- " + result.getName()
				+ "---------------------------------------- ");
		if(debug)System.out.println(Arrays.toString(result.getColumnNames()));
		int count = 0;
		TableScan tr2 = new TableScan(transaction, result);
		Tuple tuple = tr2.nextTuple();
		while (tuple != null) {
			if(debug)System.out.println(Arrays.toString(tuple.getData()));
			tuple = tr2.nextTuple();
			count++;
			if (lines > 0 && count == lines)
				break;
		}
		if(debug)System.out
				.println("\n--" + count + " tuples -----------------------------------------------------------------");
	}


	public static void calcTime(RunningCallback call, String name) throws AcquireLockException {
		//if(debug)System.out.println("\n---> " + name + " Running...");
		//long lStartTime = System.nanoTime();
		
		call.run();
		
		//long lEndTime = System.nanoTime();
		//long output = lEndTime - lStartTime;
		//if(debug)System.out.println("<--- Finish " + name + " Time: " + output / 1000000 + " ms");
	}

	public static int strstr(String haystack, String needle) {
		if (haystack == null || needle == null)
			return 0;

		if (needle.length() == 0)
			return 0;

		for (int i = 0; i < haystack.length(); i++) {
			if (i + needle.length() > haystack.length())
				return -1;

			int m = i;
			for (int j = 0; j < needle.length(); j++) {
				if (needle.charAt(j) == haystack.charAt(m)) {
					if (j == needle.length() - 1)
						return i;
					m++;
				} else {
					break;
				}

			}
		}

		return -1;
	}

	public static MTable execSQL(String sql, Transaction transaction) throws AcquireLockException {
		if(debug)System.out.println("T"+transaction.getIdT()+" Exec SQL: " + sql);
		try {
			Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpcc"));
			plan.setTransaction(transaction);
			MTable result = plan.execute();
			//System.out.println(plan.getOptionalMessage());
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static class IntoVariable {
		String v;

		public String toString() {
			return v;
		}
	}

	public static IntoVariable[] execAndIntoSQL(String sql, Transaction transaction, IntoVariable... columns) throws AcquireLockException {
		if(debug)System.out.println("T"+transaction.getIdT()+" Exec SQL: " + sql);
		try {
			Plan plan = new Parse().parseSQL(sql, Kernel.getCatalog().getSchemabyName("tpcc"));
			
			plan.setTransaction(transaction);
			// showResult(transaction, plan.execute());
			MTable result = plan.execute();
			// if(debug)System.out.println(Arrays.toString(result.getColumnNames()));
			String data = getFirstResult(transaction, result);
			if (data == null) {
				///System.out.println("^^^^^ [null values result] ^^^^^");
				return null;
			}
			// if(debug)System.out.println(data);
			String values[] = data.split("\\|");
			// if(debug)System.out.println(columns.length + " - " + values.length);

			for (int i = 0; i < columns.length; i++) {
				columns[i].v = values[i];

			}
			return columns;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}


}

