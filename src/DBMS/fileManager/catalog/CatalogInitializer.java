package DBMS.fileManager.catalog;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.Column;
import DBMS.fileManager.ISchema;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.fileManager.SchemaManipulate;
import DBMS.fileManager.dataAcessManager.file.ExceededSizeBlockException;
import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
import DBMS.fileManager.dataAcessManager.file.data.FileTable;
import DBMS.fileManager.dataAcessManager.file.data.FileTuple;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.TableManipulate;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;

public class CatalogInitializer {

	private DBConnection catalogConnection;
	
	private ITransaction catalogTransaction;
	
	
	public static String SCHEMAS_NAME = "schemas";
	public static String TABLES_NAME = "tables";
	public static String COLUMNS_NAME = "columns";

	
	private ISchema defaultSchema;
	
	private TableManipulate schemasTable;
	private TableManipulate tablesTable;
	private TableManipulate columnsTable;
	
	private FileTable scatalog;
	
	public void inicializeCatalog(){
		defaultSchema = new SchemaManipulate(1,Kernel.DEFAULT_ROOT_SCHEMA_NAME, Kernel.DEFAULT_ROOT_SCHEMA_FOLDER);
		Kernel.getCatalog().setDefaultSchema(defaultSchema);
		catalogConnection = Kernel.getTransactionManager().getLocalConnection(Kernel.DEFAULT_ROOT_SCHEMA_NAME, "admin", "admin");
		catalogTransaction = Kernel.getExecuteTransactions().begin(catalogConnection,false,false);

		openSystemCatalogTables();
		loadCatalog();

	}
	

	private void openSystemCatalogTables(){
		scatalog = new FileTable(Kernel.DEFAULT_ROOT_SCHEMA_FOLDER+File.separator+"system_catalog.b", Kernel.BLOCK_SIZE);
		FileBlock block = scatalog.read(0); 
		ArrayList<FileTuple> systemTuples = null;
		
		if(block != null){
			systemTuples = block.readTuplesArray();			
		}else{
			block = new FileBlock(Kernel.BLOCK_SIZE);
		}
		
		
		if(systemTuples == null || systemTuples.isEmpty()){
			
			FileTuple schemaModel = FileTuple.build(0, SCHEMAS_NAME + TableManipulate.SEPARATOR+ "0" + TableManipulate.SEPARATOR + "0"); //lastBlockWrited,lastTupleWrited
			FileTuple tableModel = FileTuple.build(1, TABLES_NAME + TableManipulate.SEPARATOR+"0" + TableManipulate.SEPARATOR + "0");
			FileTuple columnModel = FileTuple.build(2, COLUMNS_NAME + TableManipulate.SEPARATOR+"0" + TableManipulate.SEPARATOR + "0");
			
			try {
				block.writeTuple(schemaModel);
				block.writeTuple(tableModel);
				block.writeTuple(columnModel);
				scatalog.write(block);
			} catch (ExceededSizeBlockException e) {
				e.printStackTrace();
			}
			systemTuples = block.readTuplesArray();
		}
		
		
		for (FileTuple tuple : systemTuples) {
			String data[] = tuple.getData();

			if(data[0].equals(SCHEMAS_NAME)){
				
				schemasTable = openTable(defaultSchema,0, SCHEMAS_NAME, Integer.parseInt(data[1]), Integer.parseInt(data[2]), 
						new Column("schema_id","int"),
						new Column("schema_name","varchar"));
				schemasTable.setSystemTable(true);
				
			}else if(data[0].equals(TABLES_NAME)){
				
				tablesTable = openTable(defaultSchema,1, TABLES_NAME, Integer.parseInt(data[1]), Integer.parseInt(data[2]), 
						new Column("table_id","int"),
						new Column("table_name","varchar"),
						//new Column("table_path","varchar"),
						new Column("lastBlockWrited","int"),
						new Column("lastTupleWrited","int"),
						new Column("schema_id_fk","int"));
				tablesTable.setSystemTable(true);
				
			}else if(data[0].equals(COLUMNS_NAME)){
				
				columnsTable = openTable(defaultSchema,2, COLUMNS_NAME, Integer.parseInt(data[1]), Integer.parseInt(data[2]), 
						new Column("column_id","int"),
						new Column("column_name","varchar"),
						new Column("column_type","varchar"),
						new Column("table_id_fk","int"));
				columnsTable.setSystemTable(true);
				
			}else{
				Kernel.log(this.getClass(),"Load System Catalog Error",Level.SEVERE);
			}
		}	

	}

	
	


	private void loadCatalog(){
		
		TableScan scanSchemas = new TableScan(catalogTransaction, schemasTable);
		TableScan scanTables = new TableScan(catalogTransaction, tablesTable);
		TableScan scanColumns = new TableScan(catalogTransaction, columnsTable);
	
		ITuple tupleS = scanSchemas.nextTuple();
		//File file = new File(Kernel.SCHEMAS_FOLDER);

		while(tupleS != null){
			int schemaId = Integer.parseInt(tupleS.getColunmData(schemasTable.getIdColumn("schema_id")));
			String schemaName = tupleS.getColunmData(schemasTable.getIdColumn("schema_name"));
			String path = Kernel.createDirectory(Kernel.SCHEMAS_FOLDER+File.separator+schemaName);
			ISchema schema = openSchema(schemaId, schemaName, path);
			
			scanTables.reset();
			ITuple tupleT = scanTables.nextTuple();
			while(tupleT != null){
				//LogError.save(this.getClass(),tupleT);
				int schemaIdFK = Integer.parseInt(tupleT.getColunmData(tablesTable.getIdColumn("schema_id_fk")));
			
				if(schemaIdFK == schemaId){
					int tableId = Integer.parseInt(tupleT.getColunmData(tablesTable.getIdColumn("table_id")));
					String tableName = tupleT.getColunmData(tablesTable.getIdColumn("table_name"));
					int lastBlockWrited = Integer.parseInt(tupleT.getColunmData(tablesTable.getIdColumn("lastBlockWrited")));
					int lastTupleWrited = Integer.parseInt(tupleT.getColunmData(tablesTable.getIdColumn("lastTupleWrited")));
					
					List<Column> list = new LinkedList<>();
					
					scanColumns.reset();
					ITuple tupleC = scanColumns.nextTuple();
					while(tupleC != null){
					//	LogError.save(this.getClass(),tupleC);
						int tableIdFK = Integer.parseInt(tupleC.getColunmData(columnsTable.getIdColumn("table_id_fk")));
						
						if(tableIdFK == tableId){
							int columnId = Integer.parseInt(tupleC.getColunmData(columnsTable.getIdColumn("column_id")));
							String columnName = tupleC.getColunmData(columnsTable.getIdColumn("column_name"));							
							String columnType = tupleC.getColunmData(columnsTable.getIdColumn("column_type"));
							Column column = new Column(columnId,columnName,columnType,tableId);
							list.add(column);
						}
						
						tupleC = scanColumns.nextTuple();
					}
					
					openTable(schema,tableId, tableName, lastBlockWrited, lastTupleWrited, list.toArray(new Column[list.size()]));
					
					
				}
				
				
				tupleT = scanTables.nextTuple();
			}
			
			
			tupleS = scanSchemas.nextTuple();
		}
		
	}
	
	
	public void updateSystemTable(TableManipulate table) throws ExceededSizeBlockException{
		FileBlock block = scatalog.read(0); 
		FileBlock newBlock = new FileBlock(Kernel.BLOCK_SIZE);
		newBlock.setId(0);
		ArrayList<FileTuple> systemTuples = block.readTuplesArray();
		for (FileTuple tuple : systemTuples) {
			String data[] = tuple.getData();
			if(data[0].equals(table.getName())){
			//	LogError.save(this.getClass(),"syc table*: " + table.getControlTupleString());
				FileTuple updated = FileTuple.build(table.getTableID(), table.getName() + TableManipulate.SEPARATOR+ table.getLastBlockWrited() + TableManipulate.SEPARATOR + table.getLastTupleWrited()); 
				newBlock.writeTuple(updated);
			}else{
				newBlock.writeTuple(tuple);
			}
			
		}
		scatalog.write(newBlock);
	}
	
	public void updateTable(TableManipulate table){
		if(table.isSystemTable()){
			try {
				updateSystemTable(table);
			} catch (ExceededSizeBlockException e) {
				e.printStackTrace();
			}
			return;
		}
		
		TableScan tableScan = new TableScan(catalogTransaction, tablesTable);
		ITuple tuple = tableScan.nextTuple();
		while(tuple != null){
			int tableId = Integer.parseInt(tuple.getColunmData(tablesTable.getIdColumn("table_id")));
			if(tableId == table.getTableID()){
			//	LogError.save(this.getClass(),"syc table: " + table.getControlTupleString());
				ObjectDatabaseId obj = new ObjectDatabaseId(
						String.valueOf(tablesTable.getSchemaManipulate().getId()), 
						String.valueOf(tablesTable.getTableID()), 
						String.valueOf(tableScan.getAtualBlock()), 
						String.valueOf(tuple.getId()));
				
				tuple.setData(table.getControlTuple().getData());
				IPage page = tablesTable.updateTuple(catalogTransaction, tuple, obj);
				Kernel.getBufferManager().getBufferPolicy().flush(page);
			
			}
			tuple = tableScan.nextTuple();
		}
		
		
		
	}
	
	
	public void removeTable(TableManipulate table){
		TableScan tableScan = new TableScan(catalogTransaction, tablesTable);
		ITuple tuple = tableScan.nextTuple();
		while(tuple != null){
			int tableId = Integer.parseInt(tuple.getColunmData(tablesTable.getIdColumn("table_id")));
			if(tableId == table.getTableID()){
				ObjectDatabaseId obj = new ObjectDatabaseId(
						String.valueOf(tablesTable.getSchemaManipulate().getId()), 
						String.valueOf(tablesTable.getTableID()), 
						String.valueOf(tableScan.getAtualBlock()), 
						String.valueOf(tuple.getId()));
				Kernel.log(this.getClass(),"Delete: " + tableId + " name: " + table.getName(),Level.WARNING);
				IPage page = tablesTable.deleteTuple(catalogTransaction, obj);
				Kernel.getBufferManager().getBufferPolicy().flush(page);
			
			}
			tuple = tableScan.nextTuple();
		}
	}
	
	public ITable createTable(ISchema schema, String name, Column... columns ){
		TableScan scanTables = new TableScan(catalogTransaction, tablesTable);
		ITuple tupleT = scanTables.nextTuple();
		int tableIdMax = 5;
		while(tupleT != null){
			int tableId = Integer.parseInt(tupleT.getColunmData(tablesTable.getIdColumn("table_id")));
			if(tableId > tableIdMax){
				tableIdMax = tableId;
			}
			tupleT = scanTables.nextTuple();
		}
		tableIdMax++;
		
		TableScan scanColumns = new TableScan(catalogTransaction, columnsTable);
		ITuple tupleC = scanColumns.nextTuple();
		
		int columnIdMax = 0;
		while(tupleC != null){
			int columnId = Integer.parseInt(tupleC.getColunmData(columnsTable.getIdColumn("column_id")));
			if(columnId > columnIdMax){
				columnIdMax = columnId;
			}	
			tupleC = scanColumns.nextTuple();
		}
			
		for (Column column : columns) {
			
			columnIdMax++;
			column.setTableId(tableIdMax);
			column.setId(columnIdMax);
			IPage page = columnsTable.writeTuple(catalogTransaction,column.toTuple());
			Kernel.getBufferManager().getBufferPolicy().flush(page);
		}
		
		
		TableManipulate table = openTable(schema,tableIdMax, name, 0, 0, columns);
		IPage page = tablesTable.writeTuple(catalogTransaction, table.getControlTupleString());
		Kernel.getBufferManager().getBufferPolicy().flush(page);
		return table;
	}
	
	
	
	
	private TableManipulate openTable(ISchema schema, int id, String name, int lastBlockWrited, int lastTupleWrited, Column... columns){
		TableManipulate table = (TableManipulate) TableManipulate.getInstance(name, schema);
		table.open(id, columns, lastBlockWrited, lastTupleWrited);
		return table;	
	}
	
	
	public ISchema createSchema(String name){
		String path = Kernel.createDirectory(Kernel.SCHEMAS_FOLDER+File.separator+name);
		int id = 0;
		for (ISchema s  : Kernel.getCatalog().getShemas()) {
			if(s.getId() > id){
				id = s.getId();
			}
		}
		id++;
		ISchema schema = openSchema(id, name, path);
		IPage page = schemasTable.writeTuple(catalogTransaction, schema.toString());
		Kernel.getBufferManager().getBufferPolicy().flush(page);
		return schema;
		
	}

	
	
	private ISchema openSchema(int id, String name, String path){
		ISchema schema = new SchemaManipulate(id, name,path);
		Kernel.getCatalog().addShema(schema);
		Kernel.log(this.getClass(),"open schema: " + schema + " path: " + path,Level.CONFIG);
		return schema;
	}
	

	
}	

