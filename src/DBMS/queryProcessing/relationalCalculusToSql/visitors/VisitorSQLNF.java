package DBMS.queryProcessing.relationalCalculusToSql.visitors;
import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.*;

public class VisitorSQLNF implements VisitorFormula{
	public void visit(Query n){
		n.f = n.f.accept(this);
	}

	public Formula visit(Implication n){
		Formula f = new Or(new Not(n.f1), n.f2);
		return f.accept(this);
		
	}

	public Formula visit(And n){
		n.f1 = n.f1.accept(this);
		n.f2 = n.f2.accept(this);
		return n;
	}

	public Formula visit(Or n){
		n.f1 = n.f1.accept(this);
		n.f2 = n.f2.accept(this);
		return n;	
	}

	public Formula visit(Not n){
		Formula f;
		Formula f1;
		Formula f2;

		if(n.f instanceof And){
			f1 = ((And) n.f).f1;
			f2 = ((And) n.f).f2;
			f = new Or(new Not(f1), new Not(f2));
			return f.accept(this);
		}

		else if(n.f instanceof Or){
			f1 = ((Or) n.f).f1;
			f2 = ((Or) n.f).f2;
			f = new And(new Not(f1), new Not(f2));
			return f.accept(this);
		}

		else if(n.f instanceof Implication){
			f1 = ((Implication) n.f).f1;
			f2 = ((Implication) n.f).f2;
			f = new And(f1, new Not(f2));
			return f.accept(this);	
		}

		else if(n.f instanceof Not){
			return ((Not) n.f).f.accept(this);
		}
		//TODO: REVER ESSA REGRA
		else if(n.f instanceof ForAll){
			f = new Exists(((ForAll) n.f).tuple, new Not(((ForAll) n.f).f));
			return f.accept(this);
		}

		else if(n.f instanceof InnerFormula){
			((InnerFormula) n.f).f = new Not(((InnerFormula) n.f).f);;
			return ((InnerFormula) n.f).accept(this);
		}
		else if (n.f instanceof AtomicFormulaAttOpAtt){
			AtomicFormulaAttOpAtt temp = ((AtomicFormulaAttOpAtt) n.f);
			String newOp = "";
			
			if(temp.op.equals("=")){ newOp = "!="; }
			else if (temp.op.equals("!=") || temp.op.equals("<>")){ newOp = "="; }
			else if (temp.op.equals("<")){ newOp = ">="; }
			else if (temp.op.equals("<=")){ newOp = ">"; }
			else if (temp.op.equals(">")){ newOp = "<="; }
			else if (temp.op.equals(">=")){ newOp = "<"; }
			else{
				 throw new IllegalArgumentException("Something wrong with some operator");
			}
			
			temp.op = newOp;
			return temp;
		}
		else if (n.f instanceof AtomicFormulaAttOpConst){
			AtomicFormulaAttOpConst temp = ((AtomicFormulaAttOpConst) n.f);
			String newOp = "";

			if(temp.op.equals("=")){ newOp = "!="; }
			else if (temp.op.equals("!=") || temp.op.equals("<>")){ newOp = "="; }
			else if (temp.op.equals("<")){ newOp = ">="; }
			else if (temp.op.equals("<=")){ newOp = ">"; }
			else if (temp.op.equals(">")){ newOp = "<="; }
			else if (temp.op.equals(">=")){ newOp = "<"; }
			else{
				 throw new IllegalArgumentException("Something wrong with some operator");
			}
			
			temp.op = newOp;
			return temp;
		}
		else{
			n.f = n.f.accept(this);
			return n;
		}
	}


	public Formula visit(Exists n){
		n.f = n.f.accept(this);
		return n;
	}

	public Formula visit(ForAll n){
		Formula f;
		f = new Not(new Exists(n.tuple, new Not(n.f)));
		return f.accept(this);
	}

	public Formula visit(InnerFormula n){
		n.f = n.f.accept(this);
		return n;
	}

	public Formula visit(AtomicFormulaAttOpAtt n){
		return n;
	}

	public Formula visit(AtomicFormulaAttOpConst n){
		return n;
	}

	public Formula visit(AtomicFormulaIsA n){
		return n;
	}
}