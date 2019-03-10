package DBMS.queryProcessing.relationalCalculusToSql.visitors;

import java.util.HashSet;

public class ErrorLog {
	private HashSet<String> scopeErrors;
	private HashSet<String> formulaErrors;
	
	public ErrorLog(){
		this.scopeErrors = new HashSet<String>();
		this.formulaErrors = new HashSet<String>();
	}
	
	public void addScopeError(String e){
		this.scopeErrors.add(e);
	}
	
	public void addFormulaError(String e){
		this.formulaErrors.add(e);
	}
	
	public boolean hasFormulaError(){
		return !formulaErrors.isEmpty();
	}
	
	public boolean hasScopeError(){
		return !scopeErrors.isEmpty();
	}
	
	public HashSet<String> getFormulaErrors(){
		return this.formulaErrors;
	}
	
	public HashSet<String> getScopeErrors(){
		return scopeErrors;
	}
	
}
