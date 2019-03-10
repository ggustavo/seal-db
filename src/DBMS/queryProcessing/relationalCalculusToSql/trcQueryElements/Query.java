package DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements;
import java.util.List;

import DBMS.queryProcessing.relationalCalculusToSql.visitors.*;

public class Query{
	public Formula f;
	public List <TupleProjection> tpl;

	public Query(List <TupleProjection> tpl, Formula f){
		this.tpl = tpl;
		this.f = f;
	}

	public void accept(Visitor v){
		v.visit(this);
	}

	public String accept(VisitorString v){
		return v.visit(this);
	}


	public void accept(VisitorFormula v){
		v.visit(this);
	}
	
	

}