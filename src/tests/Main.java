//package tests;
//
//import java.sql.SQLException;
//
//import DBMS.Kernel;
//import DBMS.fileManager.catalog.InitializerListen;
//import DBMS.queryProcessing.ExtendedRelationalAlgebraAPI;
//import DBMS.queryProcessing.Tuple;
//import DBMS.queryProcessing.parse.Parse;
//import DBMS.queryProcessing.queryEngine.Plan;
//import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
//import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
//import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
//import DBMS.transactionManager.Transaction;
//import DBMS.transactionManager.TransactionRunnable;
//
//public class Main {
//	
//	public static void main(String[] args) {
//		
//		
//		
//
//		
//		Kernel.getInitializer().setInitializerListen(new InitializerListen() {
//			
//			@Override
//			public void afterStartCatalog(Transaction systemTransaction) {
//				createSchema(systemTransaction);				
//			}
//		});
//		
//		Kernel.start();
//			
//		 Kernel.getExecuteTransactions().execute(new TransactionRunnable() {
//				
//				@Override
//				public void run(Transaction transaction) {
//					
//					///System.out.println(Kernel.getCatalog().show());
//					
//					//inserts(transaction);
//					
//					Plan plan = new Plan(transaction);
//	
////					try {
////						Plan planSql = new Parse().parseSQL("select * from employee where employee_id = 3", Kernel.getCatalog().getSchemabyName("company"));
////						planSql.setTransaction(transaction);
////						System.out.println();
////						System.out.println("--> "+planSql.getRoot().getName());
////						System.out.println();
////						
////					} catch (SQLException e) {
////						e.printStackTrace();
////					}
//					
//					TableOperation usuario = ExtendedRelationalAlgebraAPI.newTable(
//							plan, 
//							Kernel.getCatalog().getSchemabyName("company"), "employee");
//					
//					ProjectionOperation projection = ExtendedRelationalAlgebraAPI.newProjection(plan, "employee_name","employee_id");
//					
//					projection.setLeft(usuario);
//					
//					plan.addOperation(projection);
//
//					TableScan t = new TableScan(transaction, plan.execute());
//					Tuple tuple = null;
//					while((tuple = t.nextTuple())!=null) {
//						System.out.println("- " + tuple);
//					}
//					
//					
//					transaction.commit();
//					System.out.println(Kernel.getMemoryAcessManager().getAlgorithm().showStatics());
//					Kernel.getMemoryAcessManager().getAlgorithm().saveData();
//					Kernel.getMemoryAcessManager().closeLog();
//					//System.out.println(Kernel.getCatalog().show());
//					//Kernel.getRecoveryManager().printLog();
//				}
//
//			
//				private void inserts(Transaction transaction) {
//					Plan insert;
//					try {
//						insert = new Parse().parseSQL(
//								" INSERT INTO employee (employee_id, employee_name, salary, branch_id_fk) VALUES " +
//								" ( 1 , 'Mateus Lopes',     500,  1 ), " +
//								" ( 2 , 'Ronaldo Santos',   3500, 1 ), " +
//								" ( 3 , 'Lucas Vieira',     5500, 1 ); ", 
//								 Kernel.getCatalog().getSchemabyName("company"));
//						insert.setTransaction(transaction);
//						insert.execute();
//						System.out.println("--> "+insert.getOptionalMessage());
//						System.out.println();
//					} catch (SQLException e) {
//						e.printStackTrace();
//					}
//					
//				}
//				
//				@Override
//				public void onFail(Transaction transaction, Exception e) {
//					transaction.abort();
//					
//				}
//			});
//		
//	}
//	
//	private static void createSchema(Transaction transaction) {
//		try {
//			Plan createDatabase = new Parse().parseSQL(
//					"create database company", 
//					Kernel.getCatalog().getDefaultSchema());
//			createDatabase.setTransaction(transaction);
//			System.out.println();
//			createDatabase.execute();
//			System.out.println("--> "+createDatabase.getOptionalMessage());
//			System.out.println();
//					
//			Plan createTable = new Parse().parseSQL(
//					"create table employee (employee_id int, employee_name varchar, salary float, branch_id_fk int);", 
//					 Kernel.getCatalog().getSchemabyName("company"));
//			createTable.setTransaction(transaction);
//			createTable.execute();
//			System.out.println("--> "+createTable.getOptionalMessage());
//	
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
//
//
//	
//}
