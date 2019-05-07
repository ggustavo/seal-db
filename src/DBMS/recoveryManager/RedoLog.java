package DBMS.recoveryManager;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Schema;
import DBMS.fileManager.dataAcessManager.file.log.FileRedoLog;
import DBMS.fileManager.dataAcessManager.file.log.IndexLog;
import DBMS.fileManager.dataAcessManager.file.log.LogHandle;
import DBMS.fileManager.dataAcessManager.file.log.LogInterator;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.TransactionOperation;

public class RedoLog implements IRecoveryManager {

	private LogHandle logHandle;
	private int CURRENT_LSN = 0;
	private final String TUPLE_ID_SEPARATOR = "Æ";
	private boolean debug = false;
	
	public void start(String file) {
		logHandle = new IndexLog(file);
		CURRENT_LSN = logHandle.readLastLSN();
		
		if (Kernel.getFinalizeStateDatabase().equals(Kernel.DATABASE_FINALIZE_STATE_ERROR)) {
			if(Kernel.ENABLE_RECOVERY){
				Kernel.log(this.getClass(),"Start recovery process...",Level.WARNING);
				recovery();	
				Kernel.log(this.getClass(),"Finish recovery process...",Level.WARNING);
			}
		} else {
			Kernel.log(this.getClass(),"Normal startup without recovery...",Level.WARNING);
		}

		Kernel.setFinalizeStateDatabase(Kernel.DATABASE_FINALIZE_STATE_ERROR);
		//if(Kernel.ENABLE_RECOVERY)addCheckPoint();
		
	}

	public void safeFinalize() {
		Kernel.getScheduler().abortAll();
		//if(Kernel.ENABLE_RECOVERY)addCheckPoint();
		Kernel.setFinalizeStateDatabase(Kernel.DATABASE_FINALIZE_STATE_OK);
	}
	
	
	
	private int cDelete = 0;
	private int cInserts= 0;
	private int cUpdates = 0;
	public void recovery() {
		
		logHandle.interator(new LogInterator() {
			
			@Override
			public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {
				
				if(debug) {
					System.out.println("---------------------------------------");
					System.out.println("LSN: "+lsn);
					System.out.println("Transaction: T" +trasaction);
					System.out.println("operation: " +operation);
					System.out.println("log file pointer: "+filePointer);
					System.out.println("object: " +obj);					
				}
				

				if (operation == MTable.DELETE) {
					cDelete++;
					String id = obj.split(TUPLE_ID_SEPARATOR)[0];
				//	String stringTuple = obj.split(TUPLE_ID_SEPARATOR)[1];
					
					String schemaID = id.split("-")[0];
					String tableID = id.split("-")[1];
					String tupleID = id.split("-")[2];
						
					Schema schema = Kernel.getCatalog().getSchemabyId(schemaID);
					MTable table = schema.getTableById(tableID);
			
					table.getTuplesHash().remove(tupleID);
					
				} 
				
				if (operation == MTable.UPDATE) {
					cUpdates++;
					String id = obj.split(TUPLE_ID_SEPARATOR)[0];
					String stringTuple = obj.split(TUPLE_ID_SEPARATOR)[1];
					
					String schemaID = id.split("-")[0];
					String tableID = id.split("-")[1];
					String tupleID = id.split("-")[2];
					
					Schema schema = Kernel.getCatalog().getSchemabyId(schemaID);
					MTable table = schema.getTableById(tableID);
			
					Tuple t = table.getTuplesHash().get(tupleID);
					t.setData(stringTuple.split("\\"+MTable.SEPARATOR));

					
				}
				
				if (operation == MTable.INSERT) {
					cInserts++;
					String id = obj.split(TUPLE_ID_SEPARATOR)[0];
					String stringTuple = obj.split(TUPLE_ID_SEPARATOR)[1];
					
					String schemaID = id.split("-")[0];
					String tableID = id.split("-")[1];
					String tupleID = id.split("-")[2];
						
					Schema schema = Kernel.getCatalog().getSchemabyId(schemaID);
					MTable table = schema.getTableById(tableID);
					
					Tuple tuple = new Tuple(table, tupleID, stringTuple.split("\\"+MTable.SEPARATOR));
					table.getTuplesHash().put(tupleID, tuple);
					table.setLastTupleWrited(Integer.parseInt(tupleID));
						
				}

				if(operation == TransactionOperation.BEGIN_TRANSACTION) {

				}
				if(operation == TransactionOperation.COMMIT_TRANSACTION) {
					
				}
				
				if(obj == null)return LogInterator.STOP;
				return LogInterator.NEXT;
			}
			
			@Override
			public void error(Exception e) {
				e.printStackTrace();
				
			}
		},false);
		
		//Kernel.log(this.getClass(),"Redo " + cTransactions + " Transactions",Level.WARNING);
		Kernel.log(this.getClass(),"Redo " + cInserts + " Inserts Records",Level.WARNING);
		Kernel.log(this.getClass(),"Redo " + cUpdates + " Updates Records",Level.WARNING);
		Kernel.log(this.getClass(),"Redo " + cDelete + " Deletes Records",Level.WARNING);
		
	}
	
	public void undoTransaction(Transaction transaction) {
		if(!Kernel.ENABLE_RECOVERY)return;
		for (int i = transaction.getOperations().size()-1; i >= 0; i--) {
			TransactionOperation operation = transaction.getOperations().get(i);
			if(operation.getTuple() != null && operation.getTuple().getTable().isTemp()) {
				if(debug)System.out.println("-- TEMP\t\t"+operation);				
			}else {
				if(debug)System.out.println(operation);

				if(operation.getType() == TransactionOperation.WRITE_TUPLE) {
					
					Tuple tuple = operation.getTuple();
					MTable table = tuple.getTable();
					
					if(operation.getTupleOperation() == MTable.DELETE) {
						
						table.getTuplesHash().put(tuple.getTupleID(), tuple);
						
						
//						if(table.isSystemTable()){
//							if(table == Kernel.getInitializer().getTables()){
//								
//							}
//							if(table == Kernel.getInitializer().getSchemas()){
//								
//								
//							}	
//						}
					}else
					if (operation.getTupleOperation() == MTable.UPDATE) {
						tuple.setData(operation.getBeforedata());
						
//						if(table.isSystemTable()){
//							if(table == Kernel.getInitializer().getTables()){
//									
//								
//							}
//							if(table == Kernel.getInitializer().getSchemas()){
//								Schema schema = Kernel.getCatalog().getSchemabyName(tuple.getColunmData(table.getIdColumn("schema_name")));
//								schema.setId(Integer.parseInt(operation.getBeforedata()[Tuple.getIdColumn(table.getColumnNames(), "schema_id")]));
//								schema.setName(operation.getBeforedata()[Tuple.getIdColumn(table.getColumnNames(), "schema_name")]);	
//							}
//						}
						
					}else
					if (operation.getTupleOperation() == MTable.INSERT) {
						
						table.getTuplesHash().remove(tuple.getTupleID());
						
//						if(table.isSystemTable()){
//							if(table == Kernel.getInitializer().getTables()){
//								String schemaID = tuple.getColunmData(table.getIdColumn("schema_id_fk"));
//								String tableName = tuple.getColunmData(table.getIdColumn("table_name"));
//								Kernel.getCatalog().getSchemabyId(schemaID).removeTable(tableName);
//								
//							}
//							if(table == Kernel.getInitializer().getSchemas()){
//								Kernel.getCatalog().removeShema(tuple.getColunmData(table.getIdColumn("schema_name")));
//							}
////							if(table == Kernel.getInitializer().getColumns()){
////								
////							}
//						}
						
					}else {
						Kernel.log(this.getClass(),"LOG OPERATION ERROR: " + transaction.getIdT(),Level.SEVERE);
					}
					
					
				}else if(operation.getType() != TransactionOperation.READ_TUPLE){
					
				}else {
					//READ!
				}
			}
			
		}
		Kernel.log(this.getClass(),"Finish Undo Transaction: " + transaction.getIdT(),Level.WARNING);
	}
	
	
	
	public synchronized void commitTransaction(Transaction transaction) {
		if(!Kernel.ENABLE_RECOVERY)return;

		for (int i = 0; i < transaction.getOperations().size(); i++) {
			TransactionOperation operation = transaction.getOperations().get(i);

			if(operation.getTuple()!= null && operation.getTuple().getTable().isTemp()) {
				if(debug)System.out.println(operation+" ...TEMP....");				
			}else if( operation.getTuple() != null ){
				
				if(operation.getTuple().getTable().isSystemTable()) {
					if(debug)System.out.println(operation+" ...SYSTEM TABLE....");	
					continue;
				}
				if(debug)System.out.println(operation);
				
				if(operation.getType() == TransactionOperation.WRITE_TUPLE) {
					
					if(operation.getTupleOperation() == MTable.DELETE) {
						logHandle.append(getNewLSN(), transaction.getIdT(), operation.getTupleOperation(), getGlobalTupleID(operation.getTuple()), getGlobalTupleID(operation.getTuple()) + TUPLE_ID_SEPARATOR + operation.getBeforedata());
					}else
					if (operation.getTupleOperation() == MTable.UPDATE) {
						logHandle.append(getNewLSN(), transaction.getIdT(), operation.getTupleOperation(), getGlobalTupleID(operation.getTuple()), getGlobalTupleID(operation.getTuple()) + TUPLE_ID_SEPARATOR + operation.getTuple().getStringData());
						
					}else
					if (operation.getTupleOperation() == MTable.INSERT) {
						logHandle.append(getNewLSN(), transaction.getIdT(), operation.getTupleOperation(), getGlobalTupleID(operation.getTuple()), getGlobalTupleID(operation.getTuple()) + TUPLE_ID_SEPARATOR + operation.getTuple().getStringData());
						
					}else {
						Kernel.log(this.getClass(),"LOG OPERATION ERROR: " + transaction.getIdT(),Level.SEVERE);
					}
					
					
				}else if(operation.getType() != TransactionOperation.READ_TUPLE){
					logHandle.append(getNewLSN(), transaction.getIdT(),operation.getType(), null, "---");
				}else {
					//READ!
				}
				
			}else {
				if(debug)System.out.println(operation +" ...NULL....");
			}
			
		}
		Kernel.log(this.getClass(),"Finish Commit Transaction: " + transaction.getIdT(),Level.WARNING);
		
	}
	
	private int getNewLSN() {
		return ++CURRENT_LSN;
	}
	
	private String getGlobalTupleID(Tuple tuple) {
		if(tuple.getTable() != null 
				&& tuple.getTable().getSchemaManipulate() != null ) {
			
			return tuple.getTable().getSchemaManipulate().getId() + "-" + tuple.getTable().getTableID() + "-"+tuple.getTupleID();
		}
		
		return null;
	}
	
	@Override
	public void printLog() {
		
		logHandle.interator(new LogInterator() {
			
			@Override
			public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {
				
				System.out.println("---------------------------------------");
				System.out.println("LSN: "+lsn);
				System.out.println("Transaction: T" +trasaction);
				System.out.println("operation: " +operation);
				System.out.println("log file pointer: "+filePointer);
				System.out.println("object: " +obj);
				
				if(obj == null)return LogInterator.STOP;
				
				return LogInterator.PREV;
			}
			
			@Override
			public void error(Exception e) {
				e.printStackTrace();
				
			}
		});
		

		
	}
	
	static class LogTransactionController{
		Integer id;
		boolean isBegin = false;
		boolean redo = false;
		public LogTransactionController(Integer id) {
			this.id = id;
		}
		@Override
		public String toString() {
			return "T" + id;
		}
	
	}
}
