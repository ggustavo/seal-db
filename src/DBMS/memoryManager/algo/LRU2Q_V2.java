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

public class LRU2Q_V2 extends Memory{
	
	public LRU2Q_V2() {
		super();
	}
	
	List<Tuple> listIn = new List<Tuple>();
	List<Tuple> listOut = new List<Tuple>();
	List<Tuple> listHot = new List<Tuple>();
	

	boolean started = false;
	
	protected int kin;
	protected int kout; 
	
	private void start(){
		started = true;
		
		kin = (int) (capacity / 4); //25%
		kout = (int) (capacity / 2); //50%
		

		System.out.println("Kin: " + kin + " Kout: " + kout);
	}
	
	
	public synchronized void request(char operation, Tuple t) {
		if(!started)start();
		
		numberOfOperation++;
		
		if(t.getNode() == null) {
			missCount++;
			increase(t.size());
			insertIn(new Node<Tuple>(t));	
			
		}else {
			
			if(t.getNode().getList() == coldList) {
				missCount++;
				coldList.remove(t.getNode());
				insertIn(t.getNode());	
			}else if(t.getNode().getList() == listIn) {
				super.hitCount++;

			}else if(t.getNode().getList() == listHot) {
				super.hitCount++;
				listHot.remove(t.getNode());
				insertHot(t.getNode());

			}else if(t.getNode().getList() == listOut) {
				super.hitCount++;
				listOut.remove(t.getNode());
				insertHot(t.getNode());

			}else {
				System.out.println("List error?");
			}
			
		}
			
	}
	
	
	
	public void replacement() {
		
		if(listIn.size() + listHot.size() == capacity) {
			
			if(listIn.size() > kin) {	
				insertOut(listIn.remove(listIn.getHead()));
			}else {
				coldList.add(listHot.remove(listHot.getHead()));
			}
			
		}
	}

	public void insertIn(Node<Tuple> p){
	
			replacement();
	
			
			listIn.add(p);

	}

	public void insertOut(Node<Tuple> p) {
			//Replacement out
			if(listOut.size() == kout){

				coldList.add(listOut.remove(listOut.getHead()));
				
			}
			listOut.add(p);
	}

	public void insertHot(Node<Tuple> p) {

			
			replacement();
			
			listHot.add(p);

	}
	

	public void remove(Tuple p) {
		
		p.getNode().getList().remove(p.getNode());
		
	}
	
	public String toString(){
		return getName();
	}

	

	@Override
	public String getName() {
		
		return "2Q: A Low Overhead High Performance Buffer Management Replacement Algorithm";
	}


	@Override
	public List<Tuple> getTuples() {
		return listIn;
	}



	public void saveData() {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
		String date = dt.format(new Date());
		
		try {

			PrintWriter logRequests = new PrintWriter(new FileWriter(new File(date + " LRU2Q-IN"  + ".pages"), true));
			Node<Tuple> node = listIn.getHead();
			while (node != null) {
			
				logRequests.println(node.getValue().getFullTupleID()+","+node.getValue().getOperation());
				node = node.getNext();
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " LRU2Q-OUT"  + ".pages"), true));
			node = listOut.getHead();
			while (node != null) {
			
				logRequests.println(node.getValue().getFullTupleID()+","+node.getValue().getOperation());
				node = node.getNext();
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " LRU2Q-HOT"  + ".pages"), true));
			node = listHot.getHead();
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
		return listIn.size() + listOut.size() + listHot.size();
	}
}
