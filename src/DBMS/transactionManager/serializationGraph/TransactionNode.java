package DBMS.transactionManager.serializationGraph;

import java.util.LinkedList;
import java.util.List;

import DBMS.transactionManager.Transaction;

public class TransactionNode {
	
	private List<Edge> edgesIn;
	private List<Edge> edgesOut;
	private Transaction transaction;
	private boolean visited;

	public TransactionNode(Transaction transaction) {
		super();
		this.edgesIn = new LinkedList<>();
		this.edgesOut = new LinkedList<>();
		this.transaction = transaction;
	}
	
	public Transaction getTransaction() {
		return transaction;
	}
	public void setTransaction(Transaction transaction) {
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
