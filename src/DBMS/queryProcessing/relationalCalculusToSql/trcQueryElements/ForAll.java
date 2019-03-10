package DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.*;

public class ForAll extends Formula{
	public String tuple;
	public Formula f;
	public ForAll(String tuple, Formula f){
		this.tuple = tuple;
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