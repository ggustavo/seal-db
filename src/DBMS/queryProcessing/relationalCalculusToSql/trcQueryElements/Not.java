package DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.*;

public class Not extends Formula{
	public Formula f;
	public Not(Formula f){
		this.f = f;
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