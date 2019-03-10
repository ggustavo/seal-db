package DBMS.queryProcessing.relationalCalculusToSql.visitors;
import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.*;

public class VisitorToString implements Visitor{
	public String stringResult;

	public void visit(Query n){
		this.stringResult = "{";
		int i = 1; 
		for(TupleProjection t : n.tpl){
			this.stringResult += t.tupleName + "." + t.attribute;
			if(i != n.tpl.size()){
				stringResult += ", ";
			}
			i++;
		}

		this.stringResult += " | ";

		n.f.accept(this);

		this.stringResult += "}";
	}
	public void	visit(Implication n){
		n.f1.accept(this);
		this.stringResult += " -> ";
		n.f2.accept(this);

	}
	public void	visit(And n){
		n.f1.accept(this);
		this.stringResult += " AND ";
		n.f2.accept(this);
	}
	public void	visit(Or n){
		n.f1.accept(this);
		this.stringResult += " OR ";
		n.f2.accept(this);
	}
	public void	visit(Not n){ 
		this.stringResult += " NOT ";
		this.stringResult += "( ";
		n.f.accept(this);
		this.stringResult += " )";
	}
	public void	visit(Exists n){
		this.stringResult += "(EXISTS " + n.tuple + ")";
		this.stringResult += "(";
		n.f.accept(this);
		this.stringResult += ")";
	}
	public void	visit(ForAll n){
		this.stringResult += "(FORALL " + n.tuple + ")";
		this.stringResult += "(";
		n.f.accept(this);
		this.stringResult += ")";	
	}

	public void	visit(InnerFormula n){
		this.stringResult += "( ";
		n.f.accept(this);
		this.stringResult += " )";	
	}


	public void	visit(AtomicFormulaAttOpAtt n){
		n.t1.accept(this);
		this.stringResult += " " + n.op + " ";
		n.t2.accept(this);
	}
	public void	visit(AtomicFormulaAttOpConst n){
		n.t.accept(this);
		this.stringResult += " " + n.op + " ";
		n.c.accept(this);
	}
	public void	visit(AtomicFormulaIsA n){
		this.stringResult += n.table + "(" + n.tuple + ")";
	}
	public void visit(TupleProjection n){
		this.stringResult += n.tupleName + "." + n.attribute;
	}
	public void visit(Constant n){
		this.stringResult += n.c;	
	}
}