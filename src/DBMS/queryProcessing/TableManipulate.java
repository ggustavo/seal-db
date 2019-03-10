package DBMS.queryProcessing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;
import DBMS.fileManager.Column;
import DBMS.fileManager.ISchema;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.fileManager.dataAcessManager.file.ExceededSizeBlockException;
import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
import DBMS.fileManager.dataAcessManager.file.data.FileTable;
import DBMS.fileManager.dataAcessManager.file.data.FileTuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.Lock;
import DBMS.transactionManager.TransactionOperation;



public class TableManipulate implements ITable{

	public final static String SEPARATOR = "|";
	public final static String SUB_SEPARATOR = ";";

	
	private int tableID;
	private String name;
	private String path;
	//private int numberOfBlocks;
	private int lastTupleWrited;
	
	private Column[] columns;
	
	private int lastBlockWrited;
	
	private boolean temp = false;
	private boolean systemTable = false;
	
	private ISchema schemaManipulate;
	private FileTable fileTable;

	public static int TABLE_ID_COUNT = 100;
	
	private synchronized int getNewId(){
		TABLE_ID_COUNT++;
		return TABLE_ID_COUNT;
	}
	
	TableManipulate(String name,String path,ISchema schemaManipulate) {
		fileTable = new FileTable(path, Kernel.BLOCK_SIZE);
		tableID = getNewId();
		this.path = path;
		this.name = name.trim();	
		this.schemaManipulate = schemaManipulate;
		schemaManipulate.addTable(this);
	}
	
	public void open(int id, Column[] columns, int lastBlockWrited ,int lastTupleWrited){
		this.tableID = id;
		this.columns = columns;
		this.lastTupleWrited = lastTupleWrited;
		this.lastBlockWrited = lastBlockWrited;
		Kernel.log(this.getClass(),"open table:"+getControlTupleString()+" columns: " + Arrays.toString(columns),Level.CONFIG);
	
	}
	
//	public void openWithTuple(FileTuple fileTuple, Column[] columns){
//		LogError.save(this.getClass(),"open table: "+getControlTupleString()+" columns: " + columns);
//		String data[] = fileTuple.getData();
//		this.columns = columns;
//		tableID = Integer.parseInt(data[0]);
//		name = data[1].trim();
//		path = data[2];
//		lastTupleWrited = Integer.parseInt(data[3]);
//		lastBlockWrited = Integer.parseInt(data[4]);
//		LogError.save(this.getClass(),"open table: "+getControlTupleString()+" columns: " + Arrays.toString(columns));
//	}
	
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
		if(!isTemp())Kernel.getCatalagInitializer().updateTable(this);
	}
	
	public FileTuple getControlTuple(){
		FileTuple fileTuple = FileTuple.build(tableID,getControlTupleString()) ;
		return fileTuple;
	}
	public String getControlTupleString(){
		String tuple = "";
		tuple += tableID + SEPARATOR;
		tuple += name + SEPARATOR;
		tuple += lastBlockWrited +  SEPARATOR;
		tuple += lastTupleWrited +  SEPARATOR;
		tuple += schemaManipulate.getId(); //schema_id_fk
		return tuple;
	}
	
	
	public static ITable getInstance(String name,ISchema schemaManipulate){
		TableManipulate table = new TableManipulate(name, schemaManipulate.getTablesFolder()+File.separator+name+".b",schemaManipulate);
		return table;
	}
	
	public static ITable getTempInstance(String name,ISchema schemaManipulate, Column... columns){
		TableManipulate table = new TableManipulate(name,schemaManipulate.getTempFolder()+File.separator+name+".b",schemaManipulate);
		table.columns = columns;
		table.setTemp(true);
		return table;
	}
	
	
	
	@Override
	public Column[] getColumns() {
		return columns;
	}

	@Override
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
	
	
	public ITuple getTuple(ITransaction transaction,String tupleID_){
		String str[] = tupleID_.split("-"); //BlockId-RowId

		IPage page = Kernel.getBufferManager().getPage(transaction, schemaManipulate.getId() +"-"+ getTableID()+"-"+str[0] ,TransactionOperation.READ_TRANSACTION,temp);
		
		ArrayList<ITuple> tuples = getTuplesFromBlock(transaction, page,true);	
		int id = Integer.parseInt(str[1]); 
		
		if(tuples==null||id>=tuples.size()) {
			page.setTuplesCache(null); //TODO
			return null;
		}
		
		ITuple t = tuples.get(id);
		
		if(id==tuples.size()-1) {
			//System.out.println("FGRE");
			page.setTuplesCache(null); //TODO
		}
		
		ObjectDatabaseId obj = new ObjectDatabaseId(schemaManipulate.getId()+"", getTableID()+"", str[0], t.getId()+"");		
		TransactionOperation operation = transaction.lock(obj, Lock.READ_LOCK);
		if(operation==null)return null;
		
		transaction.unlock(page,obj,operation,null,null,temp);
		
		return t;
	}
	
	
	public ArrayList<ITuple> getTuplesFromBlock(ITransaction transaction, IPage page, boolean secondCache){
		
		ArrayList<ITuple> tuples = null;

		if (page != null) {
			
			
			if(secondCache && page.getTuplesCache() !=null) {
				return page.getTuplesCache();
			}
			
			
			byte block[] = page.getData();

			tuples = new ArrayList<ITuple>();
			FileBlock fileBlock = new FileBlock(block);
			if(fileBlock.getStatus()==-1)return null;
			ArrayList<FileTuple> ftuples = fileBlock.readTuplesArray();
			for (FileTuple fileTuple : ftuples) {
				tuples.add(new TupleManipulate(fileTuple.getTupleID(),fileTuple.getData()));
			}
			
			if(secondCache) {
				page.setTuplesCache(tuples);
			}else {
				page.setTuplesCache(null);
			}

		}
		
		return tuples;

	}
	
	
	public IPage deleteTuple(ITransaction transaction, ObjectDatabaseId obj){
		TransactionOperation operation = transaction.lock(obj, Lock.WRITE_LOCK);
		if(operation==null)return null;
		
		String idPage = this.getSchemaManipulate().getId()+"-"+getTableID()+"-"+obj.getBlockID();
		
		IPage page = Kernel.getBufferManager().getPage(transaction,idPage,TransactionOperation.WRITE_TRANSACTION,temp);
		
		byte[] beforeImage = page.getData().clone();
	
		ArrayList<ITuple> tuples = getTuplesFromBlock(transaction, page,false);	
		
		int idTuple = Integer.parseInt(obj.getTupleID()); 
		
		if(tuples==null||idTuple < 0){
			transaction.unlock(page,obj,operation,null,null,temp);
			return null;
		}
		
		
		FileBlock fileBlock = new FileBlock(Kernel.BLOCK_SIZE);
		
		try {
	
			for (ITuple t : tuples) {
				
				FileTuple fT = FileTuple.build(t.getId(),t.getStringData());
				if(String.valueOf(t.getId()).equals(obj.getTupleID())){
					fT.setStatus(-1);
					//LogError.save(this.getClass(),fT.toString());
				}
				fileBlock.writeTuple(fT);
				
			}
		
		} catch (ExceededSizeBlockException e) {
			
			e.printStackTrace();
		}
		
		
		page.setType(IPage.WRITE_PAGE);
		page.setData(fileBlock.getBlock());
		
		
		transaction.unlock(page,obj,operation,beforeImage,fileBlock.getBlock(),temp);
		return page;
	}
	
	public IPage updateTuple(ITransaction transaction,ITuple afterTuple, ObjectDatabaseId obj){
		
		TransactionOperation operation = transaction.lock(obj, Lock.WRITE_LOCK);
		if(operation==null)return null;
		
		String idPage = this.getSchemaManipulate().getId()+"-"+getTableID()+"-"+obj.getBlockID();
		
		IPage page = Kernel.getBufferManager().getPage(transaction,idPage,TransactionOperation.WRITE_TRANSACTION,temp);
		
		//byte[] beforeImage = page.getData().clone();
		byte[] beforeImage = page.getData();
		
		ArrayList<ITuple> tuples = getTuplesFromBlock(transaction, page,false);	
		
		int idTuple = Integer.parseInt(obj.getTupleID()); 
		
		if(tuples==null||idTuple < 0){
			transaction.unlock(page,obj,operation,null,null,temp);
			return null;
		}
		
		
		FileBlock fileBlock = new FileBlock(Kernel.BLOCK_SIZE);
		
		try {
				
			for (ITuple t : tuples) {
				if(t.getId() == afterTuple.getId()){
					ITuple beforeTuple = t;
					beforeTuple.setData(afterTuple.getData());
					beforeTuple.setId(afterTuple.getId());
				}
				
				FileTuple fT = FileTuple.build(t.getId(),t.getStringData());
				fileBlock.writeTuple(fT);
			}
		
		} catch (ExceededSizeBlockException e) {
			
			e.printStackTrace();
		}
		
		
		page.setType(IPage.WRITE_PAGE);
		page.setData(fileBlock.getBlock());
		
		
		transaction.unlock(page,obj,operation,beforeImage,fileBlock.getBlock(),temp);
		return page;
		
	}
	


	public IPage writeTuple(ITransaction transaction,String tupleData){
	
		setLastTupleWrited(getLastTupleWrited()+1);
		int tID = getLastTupleWrited();
		ObjectDatabaseId obj = new ObjectDatabaseId(schemaManipulate.getId()+"", getTableID()+"", getLastBlockWrited()+"",tID+"");
		
		TransactionOperation operation = transaction.lock(obj, Lock.WRITE_LOCK);
		if(operation==null)return null;
		
		String id = this.getSchemaManipulate().getId()+"-"+getTableID()+"-"+getLastBlockWrited();
		IPage page = Kernel.getBufferManager().getPage(transaction,id,TransactionOperation.WRITE_TRANSACTION,temp);
		
		//byte[] beforeImage = page.getData().clone();
		byte[] beforeImage = page.getData();
	
		FileBlock fileBlock = new FileBlock(page.getData());
		if(fileBlock.getStatus() == -1){
			fileBlock.setStatus(0);
			//setNumberOfBlocks(getNumberOfBlocks()+1);
		}
		FileTuple t = FileTuple.build(tID,tupleData);
		try {
		
			fileBlock.writeTuple(t);
			page.setType(IPage.WRITE_PAGE);
			page.setData(fileBlock.getBlock());
			//setNumberOfTuples(getNumberOfTuples()+1);
		} catch (ExceededSizeBlockException e) {
			setLastBlockWrited(getLastBlockWrited()+1);
			//LogError.save(this.getClass(),"PASSOU!!!" + getLastBlockWrited());
			//e.printStackTrace();
			transaction.unlock(page,obj,operation,null,null,temp);
			return writeTuple(transaction, tupleData);
		}
		
		transaction.unlock(page,obj,operation,beforeImage,fileBlock.getBlock(),temp);
		return page;
	}
	


	public static String[] lineToArray(String t){
		return t.split("\\"+SEPARATOR);
	}
	

	public String getPath() {
		return path;
	}


	public void setPath(String path) {
		this.path = path;
		syc();
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
		syc();
	}

	
	
	public void unloadCache(ITransaction transaction) {
		int numberOfBlocks = 0;
		IPage page = getBlock(transaction, numberOfBlocks);
		while(page != null && new FileBlock(page.getData()).getStatus()!=-1){
			page.setTuplesCache(null);
			numberOfBlocks++;
			page = getBlock(transaction, numberOfBlocks);
			if(page==null){
				break;
			}
		}
	}
	
	
	public int getNumberOfBlocks(ITransaction transaction) {
		int numberOfBlocks = 0;
		IPage page = getBlock(transaction, numberOfBlocks);
		while(page != null && new FileBlock(page.getData()).getStatus()!=-1){
			numberOfBlocks++;
			page = getBlock(transaction, numberOfBlocks);
			if(page==null){
				break;
			}
		}
		return numberOfBlocks;
	}
	
	
	private IPage getBlock(ITransaction transaction, int blockID){
		
		ObjectDatabaseId obj = new ObjectDatabaseId(schemaManipulate.getId()+"", getTableID()+"", blockID+"", "");
		
		TransactionOperation operation = transaction.lock(obj, Lock.READ_LOCK);
		if(operation==null)return null;
		
		IPage page = Kernel.getBufferManager().getPage(transaction,this.getSchemaManipulate().getId()+"-"+this.getTableID()+"-"+blockID,TransactionOperation.READ_TRANSACTION,temp);
		transaction.unlock(page,obj,operation,null,null,temp);
		return page;
	}

	
	public int getNumberOfTuples(ITransaction transaction) {
		int numberOfTuples = 0; 
		TableScan tbscan = new TableScan(transaction, this);
		ITuple tuple = tbscan.nextTuple();
		while(tuple!=null){
			//if(tuple.getId()%100000==0)System.out.println(tuple.getId());
			tuple = tbscan.nextTuple();
			numberOfTuples++;
		}
		return numberOfTuples;
	}


	public static String arrayToString(String data[]) {
		String s = "";
		for (String string : data) {
			s = s + string + TableManipulate.SEPARATOR;
		}
		return s;
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

	public ISchema getSchemaManipulate() {
		return schemaManipulate;
	}

	public FileTable getFileTable() {
		return fileTable;
	}

	public void setFileTable(FileTable fileTable) {
		this.fileTable = fileTable;
	}
	
	public int getLastBlockWrited() {
		return lastBlockWrited;
	}

	public void setLastBlockWrited(int lastBlockWrited) {
		this.lastBlockWrited = lastBlockWrited;
		syc();
	}
	public int getLastTupleWrited() {
		return lastTupleWrited;
	}

	public void setLastTupleWrited(int lastTupleWrited) {
		this.lastTupleWrited = lastTupleWrited;
		syc();
	}

	@Override
	public boolean close() {
		try {
			getFileTable().close();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public boolean isSystemTable() {
		return systemTable;
	}

	public void setSystemTable(boolean systemTable) {
		this.systemTable = systemTable;
	}

	
	
}
