package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;


import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.TableManipulate;
import DBMS.transactionManager.ITransaction;

public class Bucket{
		private String id;
		private ITable table;
		private ITransaction transaction;
		
		public Bucket(String id,ITransaction transaction, Column... columns) {
			this.transaction = transaction;
			this.table = TableManipulate.getTempInstance("Bucket"+hashCode()+ "("+id+")", Kernel.getCatalog().getSchemabyName(transaction.getConnection().getSchemaName()),columns);
			this.id = id;
		}
		
		
		public void add(ITuple tuple){
			table.writeTuple(transaction, tuple.getStringData());
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public ITable getTable() {
			return table;
		}
		public void setTable(ITable table) {
			this.table = table;
		}
		
		
		
	}