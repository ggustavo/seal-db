package DBMS.queryProcessing;
import java.io.Serializable;


import DBMS.memoryManager.util.Node;
import DBMS.transactionManager.Lock;



public class Tuple implements Serializable{

	private static final long serialVersionUID = 1L;
	private String data[];
	private MTable table;
	private String tupleID;
	private Node<Tuple> node;
	private char operation = Lock.READ_LOCK;
	
	public boolean isUsed = false;
	public long transactionId = -1;
	
	public Tuple(MTable table, String tupleId, String... data) {
		this.data = data;
		this.tupleID = tupleId;
		this.table = table;
	}


	public String[] getData() {
		return data;
	}
	
	public void setData(String[] data) {

			this.data = data;			
		
	}
	
	public MTable getTable() {
		return table;
	}

	public void setTable(MTable table) {
		this.table = table;
	}
	
	public String getTupleID() {
		return tupleID;
	}
	
	public String getFullTupleID() {
		String s = "";
		if(table != null && table.getSchemaManipulate()!=null) {
			s+=table.getSchemaManipulate().getId()+"-"+table.getTableID();
		}else if(table != null){
			s+="null"+"-"+table.getTableID();
		}else {
			s+="null-null";
		}
		s+="-"+tupleID;
		return s;
	}

	public void setTupleID(String tupleID) {
		this.tupleID = tupleID;
	}

	public String getStringData() {
		if(data==null)return null;
		String s="";
		
		for (String string : data) {
			s = s+string+MTable.SEPARATOR;
		}
		return s;
	}
	
	public synchronized int size() {
		int size = 0;
		for (String d : data) {
			size+=d.getBytes().length;
		}
		return size;
	}
	
	public static String genTuple(Object... data) {
		String s="";
		for (Object o : data) {
			s = s+o.toString()+MTable.SEPARATOR;
		}
		return s;
	}
	
	public static String genTuple(String... data) {
		String s="";
		for (String o : data) {
			s = s+o.toString()+MTable.SEPARATOR;
		}
		return s;
	}
	
	public static int getIdColumn(String columnNames[],String Column) {
		for (int i = 0; i < columnNames.length; i++) {
			if(columnNames[i].equals(Column)){
				return i;
			};
		}
		return -1;
	}
	
	public Tuple copy() {
		String[] copyD = new String[data.length]; 
		
		for (int i = 0; i < data.length; i++) {
			copyD[i] = data[i].trim();
		}
		
		Tuple t = new  Tuple(table, tupleID, data);
		t.setOperation(operation);
		
		return t;
	}
	
	@Override
	public String toString() {
		return getStringData();
	}

	public String getColunmData(int idAtribute){
		return data[idAtribute];
	}


	public Node<Tuple> getNode() {
		return node;
	}


	public void setNode(Node<Tuple> node) {
		this.node = node;
	}


	public char getOperation() {
		return operation;
	}


	public void setOperation(char operation) {
		this.operation = operation;
	}
	

	
}

