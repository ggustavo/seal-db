package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.parse.ParseVisitor;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.deleteCommands.DeleteOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;



public class DeleteStatementParse implements StatementParse{

	
	public Plan parse(Statement statement, ISchema schema) throws SQLException {
		
		Delete delete = (Delete)statement;

		String name = delete.getTable().getName();
		ITable table = schema.getTableByName(name);
		if(table==null)throw new SQLException(name+" not in the schema " + schema.getName());
		
		List<Condition> aovList = whereParse(delete, table);
	
		
		Plan plan = new Plan(null);
		
		
		DeleteOperation deleteOperation = new DeleteOperation();
		deleteOperation.setPlan(plan);
		deleteOperation.setAttributesOperatorsValues(aovList);

		TableOperation tableOperation = new TableOperation();
		tableOperation.setResultLeft(table);
		deleteOperation.setLeft(tableOperation);
		tableOperation.setPlan(plan);
	
		plan.setType(Plan.DELETE_TYPE);
		plan.setRoot(deleteOperation);
		
		return plan;

	}
	
	private List<Condition> whereParse(Delete delete, ITable table) throws SQLException {
		
		
		if(delete.getWhere()==null) return null;
		
     
       	List<Condition> aovList = new LinkedList<>();

        for (Condition c : getConditions(delete.getWhere())) {
			
			boolean right = table.getIdColumn(c.getRight()) == -1 ? false: true;
			boolean left = table.getIdColumn(c.getLeft())  == -1 ? false: true;;
			
			if(right && left){
				
				throw new SQLException("Invalid where condition : " + c);
				
			}else if(left){
				
				aovList.add((new Condition(removeStringAlias(
								c.getLeft()),
								c.getOperator(), 
								c.getRight())));
				
			}else if(right){
			
				aovList.add(new Condition(removeStringAlias(
								c.getRight()),
								reverseOperator(c.getOperator()), 
								c.getLeft()));
				
		
			}else{
				throw new SQLException("Where condition: " + c + " invalid");
			}
			
		}
    
        return aovList.isEmpty() ? null : aovList;
	}
	
	private String reverseOperator(String s){
		if(s.equals("<="))return s = ">=";
		if(s.equals(">="))return s = "<=";
		if(s.equals(">"))return s = "<";
		if(s.equals("<"))return s = ">";
		return s;
	}
	private String removeStringAlias(String col){
		String column[] = col.split("\\.");
		if(column.length == 2 && !col.contains("'") && !col.contains("\"")  ){
			return column[1];
		}else{
			return col;
		}
	}
	
	private List<Condition> getConditions(Expression e){
		List<Condition> conditions = new LinkedList<>();
		e.accept(new ParseVisitor() {		
			public void visit(AndExpression e) {
				e.getLeftExpression().accept(this);
				e.getRightExpression().accept(this);
			}
			public void visit(OrExpression e) {
				
			}

			public void comparator(ComparisonOperator  e, String operator) {
				conditions.add(new Condition(
						e.getLeftExpression().toString(),
						operator,
						e.getRightExpression().toString()));
			}
		});

		return conditions;
	}

}
