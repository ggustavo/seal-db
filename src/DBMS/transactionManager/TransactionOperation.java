package DBMS.transactionManager;

import java.util.Arrays;

import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;

public class TransactionOperation {

	public static final char READ_TUPLE = 'R';
	public static final char WRITE_TUPLE = 'W';
	public static final char ABORT_TRANSACTION = 'A';
	public static final char COMMIT_TRANSACTION = 'C';
	public static final char BEGIN_TRANSACTION = 'B';

	private Tuple tuple;
	private char type;
	private Transaction transaction;
	private String[] beforedata;
	private char tupleOperation;

	public TransactionOperation(Transaction transaction,Tuple tuple, char type) {
		this.transaction = transaction;
		this.tuple = tuple;
		this.type = type;
	}
	
	@Override
	public String toString() {
		String op = "-- T("+transaction.getIdT()+") ["+type+"]" ;
		
			switch (tupleOperation) {
			
			case MTable.GET:
				op+="\tGET";
				break;
			case MTable.UPDATE:
				op+="\tUPDATE";
				break;
				
			case MTable.INSERT:
				op+="\tINSERT";
				break;
			
	
			case MTable.DELETE:
				op+="\tDELETE";
				break;
				
			default:
				op+="";
				break;
			}
				
		return op +( tuple != null ? ("\tTuple("+tuple.getTupleID()+"): " + (tuple.getStringData()!=null?tuple.getStringData():"null") + (beforedata != null ? "--> before: " + Arrays.toString(beforedata) : "")) : "");
	}
	
	public Transaction getTransaction() {
		return transaction;
	}

	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}


	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public String[] getBeforedata() {
		return beforedata;
	}

	public void setBeforedata(String[] beforedata) {
		this.beforedata = beforedata;
	}

	public char getTupleOperation() {
		return tupleOperation;
	}

	public void seTupleOperation(char writeTupleOperation) {
		this.tupleOperation = writeTupleOperation;
	}
	
	
	
	
}
