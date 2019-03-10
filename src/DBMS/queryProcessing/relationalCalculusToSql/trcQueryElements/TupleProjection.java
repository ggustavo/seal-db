package DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.*;

public class TupleProjection{
	public String tupleName;
	public String attribute;
	public TupleProjection(String tupleName, String attribute){
		this.tupleName = tupleName;
		this.attribute = attribute;
	}

	public void accept(Visitor v){
		v.visit(this);
	}

	public String accept(VisitorString v){
		return v.visit(this);
	}

	
}