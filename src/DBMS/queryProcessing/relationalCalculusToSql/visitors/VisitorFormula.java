package DBMS.queryProcessing.relationalCalculusToSql.visitors;
import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.*;

public interface VisitorFormula{
	public void visit(Query n);
	public Formula visit(Implication n);
	public Formula visit(And n);
	public Formula visit(Or n);
	public Formula visit(Not n);
	public Formula visit(Exists n);
	public Formula visit(ForAll n);
	public Formula visit(AtomicFormulaAttOpAtt n);
	public Formula visit(AtomicFormulaAttOpConst n);
	public Formula visit(AtomicFormulaIsA n);
	public Formula visit(InnerFormula n);
}