package DBMS.queryProcessing.parse;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import DBMS.fileManager.Schema;
import DBMS.queryProcessing.parse.statements.CreateDatabaseStatementParse;
import DBMS.queryProcessing.parse.statements.CreateTableStatementParse;
import DBMS.queryProcessing.parse.statements.DeleteStatementParse;
import DBMS.queryProcessing.parse.statements.DropTableStatementParse;
import DBMS.queryProcessing.parse.statements.InsertStatementParse;
import DBMS.queryProcessing.parse.statements.SelectStatementParse;
import DBMS.queryProcessing.parse.statements.UpdateStatementParse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.TransactionActionsOperation;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class Parse {
	
	private static OptimizerInterceptor optimizer = new OptimizerInterceptor();
	
	public static Parse getNewInstance(){
		return new Parse();
	}
	
	public static interface NewPlanListener{
		void newPlan(Plan plan);
	}

	public List<Plan> parse(String sql , Schema schema)throws SQLException{
		return parse(sql, schema,null);
	}
	
	public Plan parseSQL(String sql , Schema schema)throws SQLException{
		return parse(sql, schema,null).get(0);
	}
	
	private void callListener(Plan plan, NewPlanListener listener){
		if(listener!= null)listener.newPlan(plan);
	}
	
	public List<Plan> parse(String sql , Schema schema, NewPlanListener listener)throws SQLException{
		List<Plan> planList = createDatabaseParse(sql, schema);
		if(planList!=null){
			callListener(planList.get(0), listener);
			return planList;
		}
		

		Plan plan = null;
		planList = new  LinkedList<>();
		
		if(compareStatement(sql, "commit")){
			plan = transactionActionParse(TransactionActionsOperation.ACTION_TRANSACTION_COMMIT,"Transaction committed successfully");
			planList.add(plan);
			callListener(plan, listener);
			return planList;
		}
		if(compareStatement(sql, "abort")){
			plan = transactionActionParse(TransactionActionsOperation.ACTION_TRANSACTION_ABORT,"Transaction aborted successfully");
			planList.add(plan);
			callListener(plan, listener);
			return planList;
		}

		Statements ss = null;
		
		try {
			ss = CCJSqlParserUtil.parseStatements(sql);
		} catch (JSQLParserException e) {
			throw new SQLException(e.toString());
		}
		
		for (int i = 0; i < ss.getStatements().size(); i++) {
			
			Statement statement = ss.getStatements().get(i);
			//System.out.println(statement.toString());
						
			if(statement instanceof Drop){
				plan = new DropTableStatementParse().parse(statement, schema);
				planList.add(plan);
				callListener(plan, listener);
				continue;
			}
			
			if(statement instanceof Select) {
				//plan = optimizer.optimizerPlan(new SelectStatementParseTests().parse(statement, schema));
				plan = optimizer.optimizerPlan(new SelectStatementParse().parse(statement, schema));
				planList.add(plan);
				callListener(plan, listener);
				continue;
			}
			if(statement instanceof Insert){
				plan = new InsertStatementParse().parse(statement, schema);	
				planList.add(plan);
				callListener(plan, listener);
				continue;
			}
			
			if(statement instanceof Update){
				plan = new UpdateStatementParse().parse(statement, schema);	
				planList.add(plan);
				callListener(plan, listener);
				continue;
			}
			
			if(statement instanceof Delete){
				plan = new DeleteStatementParse().parse(statement, schema);	
				planList.add(plan);
				callListener(plan, listener);
				continue;
			}
			
			if(statement instanceof CreateTable){		
				plan = new CreateTableStatementParse().parse(statement,schema);
				planList.add(plan);
				callListener(plan, listener);
				continue;
			}
				
		}
		
		if(!planList.isEmpty())return planList;
		return null;
	}
	
	
	
	
	
	private List<Plan> createDatabaseParse(String sql, Schema schema) throws SQLException{
		
		String reservardWord = "create database ";
		
		if(sql == null || reservardWord == null) return null;
		
	    for (int i = sql.length() - reservardWord.length(); i >= 0; i--) {
	       
	    	if (sql.regionMatches(true, i, reservardWord, 0, reservardWord.length())){
	    		String value = sql.substring(reservardWord.length(), sql.length());
	        	List<Plan> planList = new  LinkedList<>();
	    		planList.add(new CreateDatabaseStatementParse().parse(value.trim().replace(";", ""),schema));
	    		return planList;
	        }
	         
	    }
	    return null;
		
	}
	
	public List<Plan> flushBufferParse(String sql, Schema schema) throws SQLException{
		
		return null;
		
	}
	
	public List<Plan> freeBufferParse(String sql, Schema schema) throws SQLException{
		
		return null;
		
	}
	
	public Plan transactionActionParse(char acction,String msg){
		Plan plan = new Plan(null);
		TransactionActionsOperation transactionActionsOperation = new TransactionActionsOperation();
		transactionActionsOperation.setAction(acction);
		plan.setOptionalMessage(msg);
		plan.addOperation(transactionActionsOperation);
		plan.setType(Plan.TRANSACTION_TYPE);
		return plan;
	}
	
	public boolean compareStatement(String statement, String value){
		
		if( statement.toString().trim().equalsIgnoreCase(value) || statement.toString().trim().equalsIgnoreCase(value+";") ){
			return true;
		}
		
		return false;
	}
/*	
	public static void main(String[] args) {
		Parse p = new  Parse();
		try {
			
			List<Plan >plans = p.parse("Select * from tabel", null);
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
*/	
	
	
}
