package DBMS.connectionManager;

import java.util.LinkedList;
import java.util.List;

import DBMS.Kernel;
import DBMS.transactionManager.ITransaction;

public class DBConnection {
	
	
	private String schema;
	private int id;
	private transient List<ITransaction> transactions;

	public DBConnection(String schema){
		id = Kernel.getNewID(Kernel.PROPERTIES_CONNECTION_ID);
		this.schema = schema;
		transactions = new LinkedList<>();
	
	}

	public String getSchemaName() {
		return schema;
	}
	
	public int getId() {
		return id;
	}

	public List<ITransaction> getTransactions() {
		return transactions;
	}


	
}
