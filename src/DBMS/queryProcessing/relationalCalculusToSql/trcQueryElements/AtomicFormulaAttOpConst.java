package DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.*;

public class AtomicFormulaAttOpConst extends Formula{
	public TupleProjection t;
	public String op;
	public Constant c;
	public AtomicFormulaAttOpConst(String op, TupleProjection t, Constant c){
		this.op = op;
		this.t = t;
		this.c = c;
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