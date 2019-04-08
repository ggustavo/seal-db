package DBMS.memoryManager.algo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import DBMS.memoryManager.util.List;
import DBMS.memoryManager.util.Node;
import DBMS.queryProcessing.Tuple;

public class ARC extends Memory{
	
	public ARC() {
		super();
	}
	
	
	protected List<Tuple> T1 = new List<Tuple>();
	protected List<Tuple> T2 = new List<Tuple>();
	protected List<Tuple> B1 = new List<Tuple>();
	protected List<Tuple> B2 = new List<Tuple>();
	
	protected int POINTER = 0;

	int L1() {
		return B1.size() + T1.size();
	}

	int L2() {
		return B2.size() + T2.size();
	}
	
	public synchronized void request(char operation, Tuple t) {
		asserts();
		numberOfOperation++;
		
		if(t.getNode() == null) {
			super.missCount++;
			increase(t.size());
			insertT1(new Node<Tuple>(t));	
			
		}else {
			
			if(t.getNode().getList() == coldList) {
				super.missCount++;
				coldList.remove(t.getNode());
				insertT1(t.getNode());
			}else if(t.getNode().getList() == T1) {
				super.hitCount++;
				T1.remove(t.getNode());
				insertT2(t.getNode());

			}else if(t.getNode().getList() == T2) {
				super.hitCount++;
				T2.remove(t.getNode());
				insertT2(t.getNode());
				
			}else if(t.getNode().getList() == B1) {
				super.missCount++;
				int delta = B2.size() / B1.size();
				POINTER = Math.min(capacity, (POINTER + Math.max(delta, 1)));
				replace(B1); // REPLACE(p).
				B1.remove(t.getNode());
				insertT2(t.getNode());
				
			}else if(t.getNode().getList() == B2) {
				super.missCount++;
				int delta = B1.size() / B2.size();
				POINTER = Math.max(0, POINTER - Math.max(delta, 1));
				replace(B2);
				B2.remove(t.getNode());
				insertT2(t.getNode());
				
			}else {
				System.out.println("List error?");
			}
			
		}
			
	}
	
	
	public void insertT2(Node<Tuple> x) {
		T2.add(x);
	}

	
	public void insertT1(Node<Tuple> t) {
		asserts();
		
		if (B1.size() + T1.size() == capacity) { // case (i) |L1| = c: el

			if (T1.size() < capacity) { // If |T1| < c
				free(B1.remove(B1.getHead()));
				replace(null); // REPLACE(p).

			} else { // else
				Node<Tuple> r = T1.getHead();
				T1.remove(r);
				free(r); // delete LRU page of T1 and remove it from the cache.

			}
		}

		else if ((L1() < capacity) && ((L1() + L2()) >= capacity)) { // case (ii) |L1| < c and |L1|+ |L2| ≥ c:
			
			if (L1() + L2() == 2 * capacity) { // if |L1|+ |L2|= 2c
				free(B2.remove(B2.getHead()));
			}
			replace(null);
		}

		T1.add(t);
		
	}
	
	public void free(Node<Tuple> x) {
		coldList.add(x);
	}
	
	public void replace(List<Tuple> list) {

		if ((T1.size() >= 1) && ((list == B2 && T1.size() == POINTER) || (T1.size() > POINTER))) { // if (|T1| ≥ 1) and ((x ∈ B2 and |T1|= p) or (|T1| > p))
			
			Node<Tuple> r = T1.getHead(); 
			if(r==null) {
				System.out.println("TTT " + T1.size());
			}
			T1.remove(r);
			B1.add(r); 
			// then move the LRU page of T1 to the top of B1 and remove it from the cache.
			
		} else { // else move the LRU page in T2 to the top of B2 and remove it from the cache
			Node<Tuple> r = T2.getHead();
			T2.remove(r);
			B2.add(r);
		}

	}
	
	
	public void remove(Tuple p) {
		
		p.getNode().getList().remove(p.getNode());

	}
	
	public String toString() {
		return getName();
	}

	@Override
	public String getName() {

		return "Adptative Replacement Cache (ARC)";
	}


	@Override
	public List<Tuple> getTuples() {
		List<Tuple> newList = new List<>();
		
		
		
		
		return newList;
	}



	public void saveData() {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
		String date = dt.format(new Date());
		
		try {
			
			PrintWriter logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-B1"  + ".pages"), true));
			Node<Tuple> node = B1.getHead();
			while (node != null) {
			
				logRequests.println(node.getValue().getFullTupleID()+","+node.getValue().getOperation());
				node = node.getNext();
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-T1"  + ".pages"), true));
			node = T1.getHead();
			while (node != null) {
			
				logRequests.println(node.getValue().getFullTupleID()+","+node.getValue().getOperation());
				node = node.getNext();
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-T2"  + ".pages"), true));
			node = T2.getHead();
			while (node != null) {
			
				logRequests.println(node.getValue().getFullTupleID()+","+node.getValue().getOperation());
				node = node.getNext();
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " ARC-B2"  + ".pages"), true));
			node = B2.getHead();
			while (node != null) {
			
				logRequests.println(node.getValue().getFullTupleID()+","+node.getValue().getOperation());
				node = node.getNext();
			}
			logRequests.close();
		
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}


	@Override
	public int getCurrentNumberOfPages() {
		return T1.size() + T2.size();
	}
	
	
	void asserts() {
		if ((T1.size() + T2.size()) > capacity) {
			System.out.println("<<<<<< (T1+T2) exceeded the capacity >>");
		}
		if (L1() + L2() > 2 * capacity) {
			System.out.println("<<<<<< (T1+T2+B1+B2) exceeded the capacity >>");
		}
		if (T1.size() + B1.size() > capacity) {
			System.out.println("<<<<<< (T1+B1) exceeded the capacity >>");
		}
		if (T2.size() + B2.size() > 2 * capacity) {
			System.out.println("<<<<<< (T2+B2) exceeded the capacity*2 >>");
		}
	}


}
