package DBMS.queryProcessing;

import java.util.ArrayList;

import DBMS.bufferManager.IPage;
import DBMS.fileManager.Column;
import DBMS.fileManager.ISchema;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.fileManager.dataAcessManager.file.data.FileTable;
import DBMS.transactionManager.ITransaction;

public interface ITable {
	
		public ITuple getTuple(ITransaction transaction,String rowID);
		public ArrayList<ITuple> getTuplesFromBlock(ITransaction transaction,IPage page, boolean secondCache);
		public String getPath();
		public void setPath(String path);
		public String getName();
		public int getNumberOfBlocks(ITransaction transaction);
		public int getNumberOfTuples(ITransaction transaction);
		public int getTableID();
		
		public Column[] getColumns();
		public String[] getColumnNames();
		
		public IPage writeTuple(ITransaction transaction,String tupleData);
		public IPage deleteTuple(ITransaction transaction, ObjectDatabaseId obj);
		public IPage updateTuple(ITransaction transaction,ITuple afterTuple, ObjectDatabaseId obj);

		public int getIdColumn(String column);
		public boolean isTemp();
		public void setTemp(boolean temp);
		public ISchema getSchemaManipulate();
		public void syc();
		public int getLastTupleWrited();
		boolean close();
		public FileTable getFileTable(); 
		public void unloadCache(ITransaction transaction);
		
	}
