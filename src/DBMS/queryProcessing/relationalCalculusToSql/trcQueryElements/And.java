package DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.*;

public class And extends Formula{
	public Formula f1;
	public Formula f2;
	
	public And(Formula f1, Formula f2){
		this.f1 = f1;
		this.f2 = f2;
	}

	public void accept(Visitor v){
		v.visit(this);
	}


	public String accept(VisitorString v){
		return v.visit(this);
	}
	
	public Formula accept(VisitorFormula v){
		return v.visit(this);
	}
	
}