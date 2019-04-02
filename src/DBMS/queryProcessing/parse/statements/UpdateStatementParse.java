package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import DBMS.fileManager.Schema;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.parse.ParseVisitor;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.updateCommands.UpdateOperation;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;



public class UpdateStatementParse implements StatementParse{


	
	public Plan parse(Statement statement, Schema schema) throws SQLException {
		
		Update update = (Update)statement;

		String name = update.getTables().get(0).getName();
		
		MTable table = schema.getTableByName(name);
		if(table==null)throw new SQLException(name+" not in the schema " + schema.getName());
		
		
		String[] columnsUpdate = new String[update.getColumns().size()];
		String[] columnsValues = new String[update.getExpressions().size()];
		
		for(int i=0;i< update.getColumns().size();i++){
     
            String c = update.getColumns().get(i).toString();
            String v = update.getExpressions().get(i).toString();
            			
            if(table.getIdColumn(c) == -1){
				throw new SQLException("column " + c + " not in table " +table.getName());
			}else{
				columnsUpdate[i] = c;		
				columnsValues[i] = v;
			}
        }
	
		List<Condition> aovList = whereParse(update, table);
	
		
		Plan plan = new Plan(null);
		
		
		UpdateOperation updateOperation = new UpdateOperation();
		updateOperation.setColumnsUpdate(columnsUpdate);
		updateOperation.setColumnsValues(columnsValues);
		updateOperation.setPlan(plan);
		updateOperation.setAttributesOperatorsValues(aovList);
		
		
		TableOperation tableOperation = new TableOperation();
		tableOperation.setResultLeft(table);
		updateOperation.setLeft(tableOperation);
		tableOperation.setPlan(plan);
		
		plan.setType(Plan.UPDATE_TYPE);
		plan.setRoot(updateOperation);
		return plan;
		
		
	}
	
private List<Condition> whereParse(Update update, MTable table) throws SQLException {
		
		
		if(update.getWhere()==null) return null;
		
     
       	List<Condition> aovList = new LinkedList<>();

        for (Condition c : getConditions(update.getWhere())) {
			
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
