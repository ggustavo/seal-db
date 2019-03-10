package DBMS.transactionManager.serializationGraph;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionManagerListener;

public class WaitForGraph {
	
	private List<TransactionNode> nodes;
	
	
	public WaitForGraph(){
		nodes = new LinkedList<>();
		
	}
	
	public void removeNode(TransactionNode node) {
		List<Edge> newEdges = new LinkedList<>();
		List<Edge> removedEdgesIn = new LinkedList<>();
		List<Edge> removedEdgesOut = new LinkedList<>();
		List<ITransaction> notify = new LinkedList<>();
		
		for (Edge edgeIn : node.getEdgesIn()) {
			removedEdgesIn.add(edgeIn);
			
			if(node.getEdgesOut().isEmpty()){
				if(edgeIn.getN1().getEdgesOut().size()-1 == 0){
					notify.add(edgeIn.getN1().getTransaction());
					Kernel.log(this.getClass(),"Notify: "+edgeIn.getN1().hashCode(),Level.WARNING);
				}
			}else{
				for (Edge edgeOut : node.getEdgesIn()) {
					newEdges.add(new Edge(edgeIn.getN1(), edgeOut.getN2()));
				}				
			}

		}
		
		//In
		for (Edge edge : removedEdgesIn) {
			removeEdge(edge);
		}
		
		//Out
		for (Edge edgeOut: node.getEdgesOut()) {
			removedEdgesOut.add(edgeOut);
		}
		for (Edge edge : removedEdgesOut) {
			removeEdge(edge);
		}
		
		//New
		for (Edge edge : newEdges) {
			addEdge(edge.getN1(), edge.getN2());
		}

		
		//Notify
		
		nodes.remove(node);
		for (ITransaction transaction : notify) {
			synchronized (transaction.getThread()) {
				transaction.setState(ITransaction.ACTIVE);
				transaction.getThread().notify();	
			}
		}
		
		
		
	}
	
	public void removeEdge(Edge edge){		
		edge.getN1().getEdgesOut().remove(edge);
		edge.getN2().getEdgesIn().remove(edge);
	}
	
	public TransactionNode findNode(ITransaction transaction){
		for (TransactionNode node : nodes) {
			if(node.getTransaction() == transaction)return node;
		}
		return null;
	}
	
	
	
	public void addEdge(TransactionNode n1,TransactionNode n2){
		
		Edge edge = new Edge(n1,n2);

		if(!n1.getEdgesOut().contains(edge) && !n2.getEdgesIn().contains(edge)){
			n1.getEdgesOut().add(edge);
			n2.getEdgesIn().add(edge);
			TransactionManagerListener t = Kernel.getTransactionManagerListener();
			if(t!=null)t.newGraphEdgeConflit(n1.getTransaction(), n2.getTransaction());			
		}
		
	}

	public void addEdge(ITransaction t1, ITransaction t2){
		TransactionNode n1 = findNode(t1);
		TransactionNode n2 = findNode(t2);
		addEdge(n1, n2);
	}
	
	
	public void addNode(ITransaction transaction){
		nodes.add(new TransactionNode(transaction));
		TransactionManagerListener t = Kernel.getTransactionManagerListener();
		if(t!=null)t.newTransaction(transaction);
		
	}
	public void addNode(TransactionNode node){
		nodes.add(node);
		TransactionManagerListener t = Kernel.getTransactionManagerListener();
		if(t!=null)t.newTransaction(node.getTransaction());
	}

	public boolean hasCycle(ITransaction transaction){
		if(nodes.isEmpty())return false;

		for (TransactionNode node : nodes) {
			node.setVisited(false);
		}
		TransactionNode source = findNode(transaction);
	
		return DFS(source, source);
	}
	
	public  boolean DFS(TransactionNode source, TransactionNode n2){
		if(!n2.isVisited()){
			n2.setVisited(true);
			for (Edge edge : n2.getEdgesOut()) {
				if(source == edge.getN2()){
					return true;
				}else{					
					return DFS(source, edge.getN2());
				}
			}			
		}
		return false;
	}

	public List<TransactionNode> getNodes() {
		return nodes;
	}

	public void setNodes(List<TransactionNode> nodes) {
		this.nodes = nodes;
	}
	
	
	
	
	/*
	public void printGraph(){
		for (TransactionNode transactionNode : nodes) {
			LogError.save(this.getClass(),"Node: "+ transactionNode.hashCode() );
			for (Edge edge : transactionNode.getEdgesOut()) {
				LogError.save(this.getClass(),"-> "+ edge.getN2().hashCode());
			}
			
			for (Edge edge : transactionNode.getEdgesIn()) {
				LogError.save(this.getClass(),"<- " +edge.getN1().hashCode());
			}
			
		}
	}
	
	public static void main(String[] args) {
		WaitForGraph waitForGraph = new WaitForGraph();
		TransactionNode node1 = new TransactionNode(null);
		TransactionNode node2 = new TransactionNode(null);
		
		LogError.save(this.getClass(),"Node 1: "+node1.hashCode());
		LogError.save(this.getClass(),"Node 2: "+node2.hashCode());
		
		
		waitForGraph.addNode(node1);
		waitForGraph.addNode(node2);
		
		
		
		waitForGraph.addEdge(node1, node2);
		
		waitForGraph.printGraph();
		LogError.save(this.getClass(),"----------------------");
		waitForGraph.removeNode(node2);
		waitForGraph.printGraph();
		
		//LogError.save(this.getClass(),waitForGraph.DFS(node2,node2));
		
		
	}
	*/
}
