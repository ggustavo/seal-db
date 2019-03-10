package DBMS.queryProcessing.relationalCalculusToSql.visitors;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.*;

import java.util.ArrayList;

public class VisitorToSQL implements VisitorString{
	List<String> currentTableList;
	ScopeManager sm;
	HashMap<String, HashSet<String>> dbSchema;
	ErrorLog errorLog;
	int inNot = 0;
	int inOr = 0;
	
	public VisitorToSQL(HashMap<String, HashSet<String>> dbSchema){
		this.dbSchema = dbSchema;
		this.errorLog = new ErrorLog();
	}
	
	
	public ErrorLog getErrorLog(){
		return this.errorLog;
	}
	
	public String visit(Query n){
		this.currentTableList = new ArrayList<String>();
		this.sm = new ScopeManager(dbSchema, errorLog); 
		
		this.sm = this.sm.beginScope();
		
		String cond = n.f.accept(this);
		
		
		
		String s = "SELECT DISTINCT ";
		int i = 1; 
		for(TupleProjection tuple : n.tpl){
			s += tuple.accept(this);
			if(i != n.tpl.size()){
				s += ", ";
			}
			i++;
		}

		s += " \nFROM ";

		i = 1;
		for(String table : this.currentTableList){
			s += table;
			if(i != this.currentTableList.size()){
				s += ", ";
			}
			i++;
		}

		if(cond != null){
			
			s += " \nWHERE " + cond;			
		}
		

		this.sm = this.sm.endScope();
		
		return s;		
	}
	
	public String visit(And n){
		String s1 = n.f1.accept(this);
		String s2 = n.f2.accept(this);
		String s;

		if(s1 == null && s2 == null){
			s = null; 
		}
		else if (s1 == null){
			s = s2;
		}
		else if (s2 == null){
			s = s1;
		}else {
			s = s1 + " AND " + s2;
		} 

		return s;
	}

	public String visit(Or n){
		inOr++;
		String s1 = n.f1.accept(this);
		String s2 = n.f2.accept(this);
		String s;
		inOr--;
		
		if(s1 == null && s2 == null){
			s = null; 
		}
		else if (s1 == null){
			s = s2;
		}
		else if (s2 == null){
			s = s1;
		}else {
			s = s1 + " OR " + s2;
		}

		return s;
	}

	public String visit(Not n){
		inNot++;
		String s = n.f.accept(this);
		inNot--;
		return  " NOT " + s + " ";
	}

	public String visit(Exists n){
		List<String> previousTableList = this.currentTableList;
		this.currentTableList = new ArrayList<String>();
		int tInNot = inNot;
		int tInOr = inOr;
		
		inNot = 0;
		inOr = 0;
		
		
		this.sm = this.sm.beginScope();
		this.sm.freeVariableName = n.tuple;
		String cond = n.f.accept(this);
		this.sm = this.sm.endScope();
		
		inNot = tInNot  ;
		inOr = tInOr;
		
		
		
		String s = " EXISTS ( " + " SELECT DISTINCT * FROM ";

		int i = 1;
		for(String t : this.currentTableList){
			s += t;
			if(i != this.currentTableList.size()){
				s += ", ";
			}
			i++;
		}

		if (cond != null){
			s += " WHERE " + cond;
		}

		s +=  " ) ";

		this.currentTableList = previousTableList;
		return s;
	}
	
	public String visit(InnerFormula n){
		String s = n.f.accept(this);
		return  s;	
		//return  " ( " + s + " ) ";	
	}
	
	public String visit(AtomicFormulaAttOpAtt n){
		this.sm.checkTupleAtribute(n.t1.tupleName, n.t1.attribute);
		this.sm.checkTupleAtribute(n.t2.tupleName, n.t2.attribute);
		
		String s = n.t1.tupleName + "." + n.t1.attribute + " " + n.op + " " + n.t2.tupleName + "." + n.t2.attribute;
		return  " " + s + " ";
	}
	
	public String visit(AtomicFormulaAttOpConst n){
		this.sm.checkTupleAtribute(n.t.tupleName, n.t.attribute);
		String s = n.t.tupleName + "." + n.t.attribute + " " + n.op + " " + n.c.accept(this);
		return  " " + s + " ";
	}
	
	public String visit(AtomicFormulaIsA n){
		if(inNot > 0){
			this.errorLog.addFormulaError("SafeQueryError: The formula " + n.table + "("+ n.tuple +") is negated!");
		}
		if(inOr > 0){
			this.errorLog.addFormulaError("OutOfScopeError: The formula " + n.table + "("+ n.tuple +") is associated to an OR!");
		}
		
		this.sm.bindTupleToRelation(n.tuple, n.table);
		this.currentTableList.add(n.table + " " + n.tuple);
		//this.currentTableList.add(n.table + " as " + n.tuple);
	
		
		return null;
	}
	
	public String visit(TupleProjection n){
		this.sm.checkTupleAtribute(n.tupleName, n.attribute);
		return n.tupleName + "." + n.attribute;
	}
	
	public String visit(Constant n){
		return n.c;
	}
	

	//This nodes will not appear in SQLNF
	public String visit(ForAll n){
		return null;
	}
	public String visit(Implication n){
		return null;
	}
}