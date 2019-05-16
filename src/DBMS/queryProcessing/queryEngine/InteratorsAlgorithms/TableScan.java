package DBMS.queryProcessing.queryEngine.InteratorsAlgorithms;


import java.util.Iterator;
import java.util.Map;

import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Transaction;

public class TableScan{

	
	private MTable table;
	private Map<String,Tuple> tuples;
	private Iterator<Map.Entry<String, Tuple>>  interator;
	
	private Transaction transaction;
	
	public TableScan (Transaction transaction, MTable table){
		this.table = table;
		this.transaction = transaction;
		this.tuples = table.getTuplesHash();
		this.interator = tuples.entrySet().iterator();
	}
	

	public void reset(){
		interator = tuples.entrySet().iterator();
	}
		
	public Tuple nextTuple() throws AcquireLockException {
		
		if(interator.hasNext()) {
			
			String id = interator.next().getValue().getTupleID();
			
			if(id == null)
				return null;
			
			Tuple tuple = table.getTuple(transaction, id);
			
			if(tuple==null)
				return null;
			
			
			return tuple;
		}
	
		return null;
	}

	
	public void setTransaction(Transaction transaction){
		this.transaction = transaction;
	}


}
