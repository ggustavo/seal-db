package DBMS.queryProcessing.queryEngine.InteratorsAlgorithms;


import java.util.Iterator;

import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Transaction;

public class TableScan{

	
	private MTable table;
	private java.util.List<Tuple> tuples;
	private Iterator<Tuple> interator;
	
	private Transaction transaction;
	
	public TableScan (Transaction transaction, MTable table){
		this.table = table;
		this.transaction = transaction;
		this.tuples = table.getTuples();
		this.interator = tuples.iterator();
	}
	

	public void reset(){
		interator = tuples.iterator();
	}
		
	public Tuple nextTuple() throws AcquireLockException {
		
		if(interator.hasNext()) {
			
			String id = interator.next().getTupleID();
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
