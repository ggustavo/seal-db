package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Transaction;

public class Bucket{
		private String id;
		private MTable table;
		private Transaction transaction;
		
		public Bucket(String id,Transaction transaction, Column... columns) {
			this.transaction = transaction;
			this.table = MTable.getTempInstance("Bucket"+hashCode()+ "("+id+")",columns);
			this.id = id;
		}
		
		
		public void add(Tuple tuple) throws AcquireLockException{
			table.writeTuple(transaction, tuple.getStringData());
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public MTable getTable() {
			return table;
		}
		public void setTable(MTable table) {
			this.table = table;
		}
		
		
		
	}