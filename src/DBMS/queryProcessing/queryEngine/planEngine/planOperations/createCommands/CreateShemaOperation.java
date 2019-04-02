package DBMS.queryProcessing.queryEngine.planEngine.planOperations.createCommands;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class CreateShemaOperation extends AbstractPlanOperation{

	private String schemaName;
	
	public CreateShemaOperation(){
		schemaName = "schema"+hashCode();
	}
	
	
	
	protected void executeOperation(MTable resultTable) {	
		Kernel.getInitializer().createSchema(schemaName, plan.getTransaction());
		
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
