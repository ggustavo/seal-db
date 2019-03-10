package DBMS.queryProcessing.relationalCalculusToSql.visitors;
import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.*;

public interface VisitorString{
	public String visit(Query n);
	public String visit(Implication n);
	public String visit(And n);
	public String visit(Or n);
	public String visit(Not n);
	public String visit(Exists n);
	public String visit(ForAll n);
	public String visit(InnerFormula n);
	public String visit(AtomicFormulaAttOpAtt n);
	public String visit(AtomicFormulaAttOpConst n);
	public String visit(AtomicFormulaIsA n);
	public String visit(TupleProjection n);
	public String visit(Constant n);
}