package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;

import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.deleteCommands.DropTableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;


public class DropTableStatementParse implements StatementParse{

	public DropTableStatementParse() {
		
	}

	@Override
	public Plan parse(Statement statement, ISchema schema) throws SQLException {
		
		Drop drop = (Drop)statement;
		
		
		String name = drop.getName().getName();
		ITable table = null;
		
		for (ITable t : schema.getTables()) {
			if(t.getName().equals(name)){
				table = t;
				break;
			}
		}
		if(table == null){
			throw new SQLException("[ERR0] Table: " + name + "  is not in schema: " + schema.getName());
		}
		
	

		Plan plan = new Plan(null);
		
		DropTableOperation dropTable = new DropTableOperation();
		dropTable.setPlan(plan);
		dropTable.setResultLeft(table);
		plan.addOperation(dropTable);
		plan.setType(Plan.DROP_TYPE);
		
		
		TableOperation tableOperation = new TableOperation();
		tableOperation.setResultLeft(table);
		dropTable.setLeft(tableOperation);
		tableOperation.setPlan(plan);
		
		plan.setType(Plan.UPDATE_TYPE);
		plan.setRoot(dropTable);
		
		return plan;
	}
	
}
