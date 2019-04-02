package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;


import DBMS.Kernel;
import DBMS.fileManager.Schema;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.createCommands.CreateShemaOperation;
import net.sf.jsqlparser.statement.Statement;



public class CreateDatabaseStatementParse implements StatementParse{

	public CreateDatabaseStatementParse() {
		
	}
	
	@Override
	public Plan parse(Statement statement, Schema schema) throws SQLException {
	
		return null;
	}

	public Plan parse(String name, Schema schema) throws SQLException {
	
		for (Schema schemaManipulate : Kernel.getCatalog().getShemas()) {
			if(schemaManipulate.getName().equals(name)){
				throw new SQLException("[ERR0] Schema: " + name + " already exists");
			}
		}
				
	
		Plan plan = new Plan(null);
		CreateShemaOperation createShema = new CreateShemaOperation();
		createShema.setSchemaName(name);
		plan.addOperation(createShema);
		plan.setType(Plan.CREATE_TYPE);
		return plan;
	}

}
