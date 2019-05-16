package DBMS.queryProcessing;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.fileManager.Schema;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Lock;
import DBMS.transactionManager.Transaction;



public class MTable{

	public final static String SEPARATOR = "|";
	public final static String SUB_SEPARATOR = ";";

	public static final char INSERT = 'I';
	public static final char UPDATE = 'U';
	public static final char DELETE = 'D';
	public static final char GET = 'G';
	
	private int tableID;
	
	private String name;
	
	private int lastTupleWrited;
	
	private Column[] columns;
	
	private boolean temp = false;
	private boolean systemTable = false;
	
	private Schema schemaManipulate;
	
	public static int TABLE_ID_COUNT = 1000;
		
	private ConcurrentHashMap<String,Tuple> tuples;
	
	private synchronized static int getNewId(){
		TABLE_ID_COUNT++;
		return TABLE_ID_COUNT;
	}
	
	MTable(String name,Schema schemaManipulate) {
		this.name = name.trim();	
		this.schemaManipulate = schemaManipulate;
	}
	
	public static String columnsToString(Column...columns){
		String s = "";
		for (Column column : columns) {
			if(!s.isEmpty())s+=SEPARATOR;
			s+= column.getId()+SUB_SEPARATOR+
				column.getName()+SUB_SEPARATOR+
				column.getType()+SUB_SEPARATOR+
				column.getTableId();
		}
		return s;
	}
	
	public static Column[] stringToColumns(String string){
		String [] sc = string.split("\\"+SEPARATOR);
		Column[] columns = new Column[sc.length];
	//	LogError.save(this.getClass(),"size: " + sc.length);
		for (int i = 0; i < columns.length; i++) {
		//	LogError.save(this.getClass(),sc[i]);
			String[] data = sc[i].split(SUB_SEPARATOR);
		//	LogError.save(this.getClass(),"subsize: " +data.length);
			columns[i] = new Column(Integer.parseInt(data[0]), data[1], data[2], Integer.parseInt(data[3]));
		}
		return columns;
	}

	public void syc(){
		if(!isTemp())Kernel.getInitializer().updateTable(this);
	}
	
	public String getControlTupleString(){
		String tuple = "";
		tuple += tableID + SEPARATOR;
		tuple += name + SEPARATOR;
		tuple += lastTupleWrited +  SEPARATOR;
		tuple += schemaManipulate.getId(); //schema_id_fk
		return tuple;
	}
	
	public static MTable getInstance(int id, String name, Schema schemaManipulate, int lastTupleWrited, Column... columns){
		MTable table = new MTable(name,schemaManipulate);
		table.tableID = id;
		table.columns = columns;
		table.lastTupleWrited = lastTupleWrited;
		schemaManipulate.addTable(table);
		table.tuples = new ConcurrentHashMap<>();
		Kernel.log(table.getClass(),"open table: " + table.getControlTupleString() + " columns: " + Arrays.toString(columns),Level.CONFIG);
		return table;
	}
	
	public static MTable getTempInstance(String name, Column... columns){
		MTable table = new MTable(name,Kernel.getCatalog().getTempSchema());
		table.columns = columns;
		table.setTemp(true);
		table.tableID = getNewId();
		table.lastTupleWrited = 0;
		table.schemaManipulate.addTable(table);
		table.tuples = new ConcurrentHashMap<>();
		//Kernel.log(table.getClass(),"open temp table: "+table.getControlTupleString()+" columns: " + Arrays.toString(columns),Level.CONFIG);
		
		return table;
	}
	
	public Column[] getColumns() {
		return columns;
	}

	public String[] getColumnNames() {
		String [] array = new String[columns.length];
		for (int i = 0; i < columns.length; i++) {
			array[i] = columns[i].getName();
		}
		return array;
	}
	
	public int getIdColumn(String columnName) {
		for (int i = 0; i < columns.length; i++) {
			if(columns[i].getName().equals(columnName)){
				return i;
			};
		}
		return -1;
	}
	
	
	public Tuple getTuple(Transaction transaction, String tupleId) throws AcquireLockException{
		
		Tuple tuple = tuples.get(tupleId); 
		if(tuple==null)return null;
		
		if(!transaction.lock(tuple, Lock.WRITE_LOCK))return null;
		
		Kernel.getMemoryAcessManager().request(Lock.READ_LOCK, tuple);
		
		transaction.unlock(Lock.READ_LOCK,GET,tuple,null,temp);
		
		return tuple;
	}
	
	
	public boolean deleteTuple(Transaction transaction, String tupleId) throws AcquireLockException{
		
		Tuple tuple = tuples.get(tupleId); 
		if(tuple==null)return false;
		
		
		if(!transaction.lock(tuple, Lock.WRITE_LOCK))return false;
		
		Kernel.getMemoryAcessManager().request(Lock.WRITE_LOCK, tuple);
		tuples.remove(tupleId);
		
		transaction.unlock(Lock.WRITE_LOCK, DELETE,tuple,null,temp);
		tuple.setData(null);
		
		return true;
	}
	
	public boolean updateTuple(Transaction transaction,String data[], String tupleId) throws AcquireLockException{
		
		Tuple tuple = tuples.get(tupleId); 
		if(tuple==null)return false;
		
		if(!transaction.lock(tuple, Lock.WRITE_LOCK))return false;
		
		String [] before = tuple.getData();
		tuple.setData(data);
		Kernel.getMemoryAcessManager().request(Lock.WRITE_LOCK, tuple);
		
		//System.out.println("before: " + arrayToString(before));
		//System.out.println("after: " + arrayToString(data));
				
		transaction.unlock(Lock.WRITE_LOCK, UPDATE,tuple,before,temp);
		
		return true;
	}
	


	public boolean writeTuple(Transaction transaction,String tupleData) throws AcquireLockException{
	
		setLastTupleWrited(getLastTupleWrited()+1);
		int id = getLastTupleWrited();
		Tuple tuple = new Tuple(this, String.valueOf(id), lineToArray(tupleData));
		

		if(!transaction.lock(tuple, Lock.WRITE_LOCK))return false;
				
		tuples.put(tuple.getTupleID(), tuple);
		Kernel.getMemoryAcessManager().request(Lock.WRITE_LOCK, tuple);
		
		transaction.unlock(Lock.WRITE_LOCK, INSERT,tuple,null,temp);
		
		return true;
	}
	

	public static String[] lineToArray(String t){
		return t.split("\\"+SEPARATOR);
	}
	

	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
		syc();
	}

	
	public int getNumberOfTuples(Transaction transaction) {
		
		return tuples.size();
	}


	public static String arrayToString(String data[]) {
		String s = "";
		for (String string : data) {
			s = s + string + MTable.SEPARATOR;
		}
		return s;
	}
	
	public Collection<Tuple> getTuples(){
		return tuples.values();
	}
	
	public  Map<String, Tuple> getTuplesHash(){
		return tuples;
	}
	
	public int getTableID() {
		return tableID;
	}


	public void setTableID(int tableID) {
		this.tableID = tableID;
		syc();
	}
	
	public String toString(){
		return this.name;
	}

	public boolean isTemp() {
		return temp;
	}

	public void setTemp(boolean temp) {
		this.temp = temp;
	}

	public Schema getSchemaManipulate() {
		return schemaManipulate;
	}
	
	public int getLastTupleWrited() {
		return lastTupleWrited;
	}

	public void setLastTupleWrited(int lastTupleWrited) {
		this.lastTupleWrited = lastTupleWrited;
		syc();
	}

	public boolean isSystemTable() {
		return systemTable;
	}

	public void setSystemTable(boolean systemTable) {
		this.systemTable = systemTable;
	}

}
