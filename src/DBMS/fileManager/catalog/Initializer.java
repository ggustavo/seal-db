package DBMS.fileManager.catalog;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.fileManager.Schema;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Transaction;

public class Initializer {

	private MTable tables;
	private MTable schemas;
	private MTable columns;
	
	private Transaction systemTransaction;
	private InitializerListen initializerListen;
	
	public void inicializeCatalog(){
		
		systemTransaction = Kernel.getExecuteTransactions().begin(false, false);
		
		Schema defaultSchema = new Schema(0, "seal-db");
		Schema tempSchema = new Schema(1, "temp");
	
		Kernel.getCatalog().setDefaultSchema(defaultSchema);
		Kernel.getCatalog().setTempSchema(tempSchema);
		
		//******************************************************************
		schemas = MTable.getInstance(0, "schemas", defaultSchema, 0, 
				new Column("schema_id","int"),
				new Column("schema_name","varchar"));

		tables = MTable.getInstance(1, "tables", defaultSchema, 0, 
				new Column("table_id","int"),
				new Column("table_name","varchar"),
				new Column("lastTupleWrited","int"),
				new Column("schema_id_fk","int"));

		columns = MTable.getInstance(2, "columns", defaultSchema, 0, 
				new Column("column_id","int"),
				new Column("column_name","varchar"),
				new Column("column_type","varchar"),
				new Column("table_id_fk","int"));
		
		schemas.setSystemTable(true);
		tables.setSystemTable(true);
		columns.setSystemTable(true);
		
		defaultSchema.addTable(schemas);
		defaultSchema.addTable(tables);
		defaultSchema.addTable(columns);
		//******************************************************************
		
		if(initializerListen != null) {
			initializerListen.afterStartSystemCatalog(systemTransaction);
		}
		
	}

	public Schema createSchema(String schema, Transaction transaction) {
		int id = -1;
		for (Schema s : Kernel.getCatalog().getShemas()) {
			if(s.getId() > id) id = s.getId();
		}
		id++;
		
		try {
			schemas.writeTuple(transaction, Tuple.genTuple(id,schema));
		} catch (AcquireLockException e) {
			e.printStackTrace();
		}
		Schema newSchema = new Schema(id, schema);
		Kernel.getCatalog().addShema(newSchema);
		return newSchema;
	}
	
	public void updateTable(MTable pointerTable) {
		
	}
	
	public void removeTable(MTable pointerTable) {
		
	}
	
	public MTable createTable(Transaction transaction, Schema schemaManipulate, String tableName, Column... cols ) {
		
		try {
			int id = -1;
			for (Schema s : Kernel.getCatalog().getShemas()) {
				//System.out.println(s.getName());
				for(MTable m : s.getTables()) {
					if(m.getTableID() > id) id = m.getTableID();
				}
			}
			
			id++;
			MTable m =  MTable.getInstance(id, tableName, schemaManipulate, 0, cols);
			
				tables.writeTuple(transaction, m.getControlTupleString());
			
	
			int lastIDCol = 0;
			
			for (Tuple column : columns.getTuples()) {
				int idCol = Integer.parseInt(column.getColunmData(columns.getIdColumn("column_id")));
				if(idCol > lastIDCol){
					lastIDCol = idCol;
				}
			}
			lastIDCol++;
			for (Column column : cols) {
				columns.writeTuple(transaction, lastIDCol + MTable.SEPARATOR + 
												column.getName()  + MTable.SEPARATOR +
												column.getType()  + MTable.SEPARATOR +
												id + MTable.SEPARATOR
				);
				lastIDCol++;
			}
			return m;
	} catch (AcquireLockException e) {
		e.printStackTrace();
		Kernel.exception(this.getClass(), e);
	}
		return null;
	}

	public MTable getTables() {
		return tables;
	}

	public void setTables(MTable tables) {
		this.tables = tables;
	}

	public MTable getSchemas() {
		return schemas;
	}

	public void setSchemas(MTable schemas) {
		this.schemas = schemas;
	}

	public MTable getColumns() {
		return columns;
	}

	public void setColumns(MTable columns) {
		this.columns = columns;
	}

	public InitializerListen getInitializerListen() {
		return initializerListen;
	}

	public void setInitializerListen(InitializerListen initializerListen) {
		this.initializerListen = initializerListen;
	}

	
	
	

}	

