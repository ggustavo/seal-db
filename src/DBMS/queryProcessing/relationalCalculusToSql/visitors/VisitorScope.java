package DBMS.queryProcessing.relationalCalculusToSql.visitors;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.*;

public class VisitorScope implements Visitor{
	int scope;
	int nOr;
	//ArrayList<RelationScope> relationScope;

	public void visit(Query n){
		scope = 0;
		nOr = 0;
	//	relationScope = new ArrayList<>();
		n.f.accept(this);

		Kernel.log(this.getClass(),"Fomula OK!!",Level.INFO);
	}

	public void	visit(And n){
		n.f1.accept(this);
		n.f2.accept(this);
	}
	public void	visit(Or n){
		nOr++;
		n.f1.accept(this);
		n.f2.accept(this);
		nOr--;
	}

	public void	visit(Not n){
		if( n.f instanceof AtomicFormulaIsA){
			throw new RuntimeException("NOT IsA Formula is Invalid!");
		}
		n.f.accept(this);
	}

	public void	visit(Exists n){
		scope++;
		n.f.accept(this);
		scope--;
	}

	public void visit(InnerFormula n){
		n.f.accept(this);
	}


	public void	visit(AtomicFormulaIsA n){
		if(this.nOr != 0){
			throw new RuntimeException("IsA formula as subformula of OR!");	
		}
	}


	//It isn't necessary to visit
	public void visit(TupleProjection n){

	}

	public void visit(Constant n){

	}

	public void	visit(AtomicFormulaAttOpAtt n){

	}

	public void	visit(AtomicFormulaAttOpConst n){

	}

	//It doesn't exist in SQLNF formulas
	public void	visit(ForAll n){

	}
	public void	visit(Implication n){

	}
}