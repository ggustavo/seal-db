package DBMS.queryProcessing.queryEngine.planEngine.planOperations.createCommands;


import java.util.LinkedList;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class CreateTableOperation extends AbstractPlanOperation{

	private String tableName;
	private LinkedList<String> columns;
	private LinkedList<String> types;
	
	
	
	protected synchronized void executeOperation(ITable resultTable) {
		
		
		ISchema schemaManipulate = Kernel.getCatalog().getSchemabyName(getPlan().getTransaction().getConnection().getSchemaName());


		Column[] cols = new Column[this.columns.size()];
		for (int i = 0; i < cols.length; i++) {
			cols[i] = new Column(columns.get(i),types.get(i));
		}
		

		Kernel.getCatalagInitializer().createTable(schemaManipulate, tableName, cols);
		plan.setOptionalMessage(tableName +" table added");
	}
	

	
	
	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}





	public Column[] getResultTupleStruct(){
		return null;
	}
	
	public String getName(){
	
		return "Create"+this.hashCode()+"("+tableName+")";
	}





	public LinkedList<String> getColumns() {
		return columns;
	}





	public void setColumns(LinkedList<String> columns) {
		this.columns = columns;
	}





	public LinkedList<String> getTypes() {
		return types;
	}





	public void setTypes(LinkedList<String> types) {
		this.types = types;
	}
	

}
