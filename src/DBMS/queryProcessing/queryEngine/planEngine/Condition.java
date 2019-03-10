package DBMS.queryProcessing.queryEngine.planEngine;



public class Condition {

	
	public static final int COLUMN_COLUMN = 1;
	public static final int COLUMN_VALUE = 2;
	public static final int CORRELATION_EXISTS = 4;
	public static final int CORRELATION_NOT_EXISTS = 5;
	
	private String atribute;
	private String operator;
	private String value;
	private int type;
	
	//AND / OR
	private String externalOperator;
	
	private String table1;
	private String table2;
	
	public Condition() {
		
	}

	public Condition(String atribute, String operator, String value) {
		super();
		this.atribute = atribute;
		this.operator = operator;
		this.value = value;
	}
	
	
	
	public Condition(String atribute, String operator, String value,
			String table1, String table2,String externalOperator) {
		super();
		this.atribute = atribute;
		this.operator = operator;
		this.value = value;
		this.externalOperator = externalOperator;
		this.table1 = table1;
		this.table2 = table2;
	}
	
	public Condition(String atribute, String operator, String value,
			String table1, String table2,String externalOperator, int type) {
		super();
		this.atribute = atribute;
		this.operator = operator;
		this.value = value;
		this.externalOperator = externalOperator;
		this.table1 = table1;
		this.table2 = table2;
		this.type = type;
	}

	


	public void reverse(){
		String s = atribute;
		atribute = value;
		this.operator = reverseOperator(operator);
		value = s;
	}
	private static String reverseOperator(String s){
		if(s.equals("<="))return s = ">=";
		if(s.equals(">="))return s = "=<";
		if(s.equals(">"))return s = "<";
		if(s.equals("<"))return s = ">";
		return s;
	}
	public String getAtribute() {
		return atribute;
	}

	public void setAtribute(String atribute) {
		this.atribute = atribute;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return atribute+" "+operator+" "+value;
	}
	
	public Condition copy(){
		
		
		Condition aov = new Condition();
		if(table1!=null)aov.setTable1(new String(table1));
		if(table2!=null)aov.setTable2(new String(table2));
		if(externalOperator!=null)aov.setExternalOperator(new String(externalOperator));

		if(atribute!=null)aov.setAtribute(new String(atribute));
		if(operator!=null)aov.setOperator(new String(operator));
		if(value!=null)aov.setValue(new String(value));
		aov.setType(type);
		return aov;
	}



	public String getExternalOperator() {
		return externalOperator;
	}



	public void setExternalOperator(String externalOperator) {
		this.externalOperator = externalOperator;
	}



	public String getTable1() {
		return table1;
	}



	public void setTable1(String table1) {
		this.table1 = table1;
	}



	public String getTable2() {
		return table2;
	}



	public void setTable2(String table2) {
		this.table2 = table2;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	public String getRight() {
		return value;
	}
	public String getLeft() {
		return atribute;
	}
	
	
}
