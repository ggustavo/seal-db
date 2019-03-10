package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.insertCommands.InsertOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;



public class InsertStatementParse implements StatementParse{


	
	public Plan parse(Statement statement, ISchema schema) throws SQLException {
		Insert insert = (Insert)statement;


		ITable table = schema.getTableByName(insert.getTable().getName());
		if(table==null)throw new SQLException(insert.getTable().getName()+" not in the schema " + schema.getName());
		
		
	
		List<Column> columns = insert.getColumns();
		String[] columnsInsert = new String[columns.size()];
		
		for (int i = 0; i < columns.size(); i++) {
			String c = columns.get(i).toString();
			if(table.getIdColumn(c) == -1){
				throw new SQLException("column " + c + " not in table " +table.getName());
			}else{
				columnsInsert[i] = c;				
			}
		}
		
		LinkedList<String[]> valuesInsert = new LinkedList<>();

		ItemsList h = insert.getItemsList();
		h.accept(new ItemsListVisitor() {
			
			@Override
			public void visit(MultiExpressionList a) {
				for(ExpressionList el : a.getExprList()) {
					visit(el);
				}
				
			}
			
			@Override
			public void visit(ExpressionList el) {
				
				String[] v = new String[el.getExpressions().size()];
				for (int j = 0; j < el.getExpressions().size(); j++) {
					v[j] = el.getExpressions().get(j).toString();
				}
				valuesInsert.add(v);
				
			}
			
			@Override
			public void visit(SubSelect arg0) {
				// TODO Auto-generated method stub
				
			}
		});

		
		Plan plan = new Plan(null);
		InsertOperation insertOperation = new InsertOperation();
		insertOperation.setColumns(columnsInsert);
		insertOperation.setValues(valuesInsert);
		insertOperation.setPlan(plan);
		
		
		TableOperation tableOperation = new TableOperation();
		tableOperation.setResultLeft(schema.getTableByName(insert.getTable().getName()));
		insertOperation.setLeft(tableOperation);
		tableOperation.setPlan(plan);
		
		plan.setType(Plan.INSERT_TYPE);
		plan.setRoot(insertOperation);
		return plan;
	}
	
	


}


