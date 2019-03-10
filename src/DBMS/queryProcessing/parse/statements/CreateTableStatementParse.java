package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;
import java.util.LinkedList;

import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.createCommands.CreateTableOperation;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class CreateTableStatementParse implements StatementParse{

	public CreateTableStatementParse() {
		
	}

	@Override
	public Plan parse(Statement statement, ISchema schema) throws SQLException {
		CreateTable create = (CreateTable)statement;
		
		
		String name = create.getTable().getName();
		
		for (ITable table : schema.getTables()) {
			if(table.getName().equals(name)){
				throw new SQLException("[ERR0] Table: " + name + " already exists in schema: " + schema.getName());
			}
		}
		
	
		LinkedList<String> columns = new LinkedList<>();
		LinkedList<String> types = new LinkedList<>();
		
		for (int i = 0; i < create.getColumnDefinitions().size(); i++) {
			ColumnDefinition c = create.getColumnDefinitions().get(i);
			columns.add(c.getColumnName().toString());
			types.add(c.getColDataType().toString());
		//	LogError.save(this.getClass(),c.getColumnName().toString());
		//	LogError.save(this.getClass(),c.getDatatype().toString());
		}
		
		Plan plan = new Plan(null);
		CreateTableOperation createTableOperation = new CreateTableOperation();
		createTableOperation.setTableName(name);
		createTableOperation.setColumns(columns);
		createTableOperation.setTypes(types);
		plan.addOperation(createTableOperation);
		plan.setType(Plan.CREATE_TYPE);
		return plan;
	}
	

}
