package DBMS.transactionManager.serializationGraph;

import java.util.LinkedList;
import java.util.List;

import DBMS.transactionManager.ITransaction;

public class TransactionNode {
	
	private List<Edge> edgesIn;
	private List<Edge> edgesOut;
	private ITransaction transaction;
	private boolean visited;

	public TransactionNode(ITransaction transaction) {
		super();
		this.edgesIn = new LinkedList<>();
		this.edgesOut = new LinkedList<>();
		this.transaction = transaction;
	}
	
	public ITransaction getTransaction() {
		return transaction;
	}
	public void setTransaction(ITransaction transaction) {
		this.transaction = transaction;
	}

	public boolean isVisited() {
		return visited;
	}

	public void setVisited(boolean visited) {
		this.visited = visited;
	}

	public List<Edge> getEdgesIn() {
		return edgesIn;
	}

	public void setEdgesIn(List<Edge> edgesIn) {
		this.edgesIn = edgesIn;
	}

	public List<Edge> getEdgesOut() {
		return edgesOut;
	}

	public void setEdgesOut(List<Edge> edgesOut) {
		this.edgesOut = edgesOut;
	}
	
	
	
	
	
}
