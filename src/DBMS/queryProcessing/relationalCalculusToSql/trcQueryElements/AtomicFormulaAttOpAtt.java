package DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.*;

public class AtomicFormulaAttOpAtt extends Formula{
	public TupleProjection t1;
	public String op;
	public TupleProjection t2;
	public AtomicFormulaAttOpAtt(String op, TupleProjection t1, TupleProjection t2){
		this.op = op;
		this.t1 = t1;
		this.t2 = t2;
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