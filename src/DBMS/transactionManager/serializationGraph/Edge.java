package DBMS.transactionManager.serializationGraph;

public class Edge {
	
	private TransactionNode n1;
	private TransactionNode n2;

	public Edge(TransactionNode n1, TransactionNode n2) {
		this.n1 = n1;
		this.n2 = n2;
	}
	public TransactionNode getN1() {
		return n1;
	}
	public void setN1(TransactionNode n1) {
		this.n1 = n1;
	}
	public TransactionNode getN2() {
		return n2;
	}
	public void setN2(TransactionNode n2) {
		this.n2 = n2;
	}
	
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Edge other = (Edge) obj;
		
		return other.n1 == n1 && other.getN2() == n2;
	}
	
	
	
}
