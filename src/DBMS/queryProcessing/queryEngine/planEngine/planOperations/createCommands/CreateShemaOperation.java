package DBMS.queryProcessing.queryEngine.planEngine.planOperations.createCommands;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class CreateShemaOperation extends AbstractPlanOperation{

	private String schemaName;
	
	public CreateShemaOperation(){
		schemaName = "schema"+hashCode();
	}
	
	
	
	protected void executeOperation(ITable resultTable) {
//		String path = Kernel.createDirectory(Kernel.SCHEMAS_FOLDER+File.separator+schemaName);
//		Kernel.getCatalog().addShema(new SchemaManipulate(schemaName, path));	
		Kernel.getCatalagInitializer().createSchema(schemaName);
		
		plan.setOptionalMessage(schemaName +" schema added");
	}
	
	
	public String getSchemaName() {
		return schemaName;
	}


	public void setSchemaName(String nameSchema) {
		this.schemaName = nameSchema;
	}


	public Column[] getResultTupleStruct(){
		return null;
	}
	
	public String getName(){
	
		return "Create"+this.hashCode()+"("+schemaName+")";
	}



	
		
		
	

	

}
