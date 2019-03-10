package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;

import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.BackupTableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import net.sf.jsqlparser.statement.Statement;

public class BackupTableOperationStatementParse implements StatementParse{

	@Override
	public Plan parse(Statement statement, ISchema schema) throws SQLException {
	
		return null;
	}

	public Plan parse(String tableName, ISchema schema) throws SQLException {

		ITable table = schema.getTableByName(tableName);
		if(table==null)throw new SQLException(tableName+" not in the schema " + schema.getName());
		
		
		Plan plan = new Plan(null);
		
		BackupTableOperation backupOperation = new BackupTableOperation();
		
		backupOperation.setPlan(plan);
		
	
		TableOperation tableOperation = new TableOperation();
		tableOperation.setResultLeft(table);
		backupOperation.setLeft(tableOperation);
		tableOperation.setPlan(plan);
		
		plan.setType(Plan.BACKUP_TYPE);
		plan.setRoot(backupOperation);
		return plan;
	}
	
	
	
	
	
}
