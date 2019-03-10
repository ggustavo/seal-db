package DBMS.queryProcessing.relationalCalculusToSql.visitors;
import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.*;

public interface Visitor{
	public void visit(Query n);
	public void	visit(Implication n);
	public void	visit(And n);
	public void	visit(Or n);
	public void	visit(Not n);
	public void	visit(Exists n);
	public void	visit(ForAll n);
	public void visit(InnerFormula n);
	public void	visit(AtomicFormulaAttOpAtt n);
	public void	visit(AtomicFormulaAttOpConst n);
	public void	visit(AtomicFormulaIsA n);
	public void visit(TupleProjection n);
	public void visit(Constant n);
}