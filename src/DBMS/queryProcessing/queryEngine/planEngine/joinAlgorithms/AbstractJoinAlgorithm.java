package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;

import java.util.List;

import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.Transaction;

public abstract class AbstractJoinAlgorithm {
	
	protected Transaction transaction;
	protected MTable tableLeft;
	protected MTable tableRight;
	protected List<Condition> attributesOperatorsValues; 
	
	
	public abstract void execute(MTable resultTable) throws AcquireLockException;
	
	
	boolean isInsertable = false;
	public boolean match(Tuple tr,Tuple ts){
		isInsertable = false;
		
		if(attributesOperatorsValues.isEmpty())return true;
		
		for (Condition aOv : attributesOperatorsValues) {
			
			//System.out.println(tr.getStringData() + " " + ts.getStringData());
			//System.out.println("COMPARE: " + tr.getColunmData(tableLeft.getIdColumn(aOv.getAtribute())) + " " + aOv.getOperator() + " " + ts.getColunmData(tableRight.getIdColumn(aOv.getValue())));
			
			if(AbstractPlanOperation.makeComparison(tr.getColunmData(tableLeft.getIdColumn(aOv.getAtribute())), aOv.getOperator(), ts.getColunmData(tableRight.getIdColumn(aOv.getValue())))){
				isInsertable = true;
			}else{
				isInsertable = false;
				break;
			}	
		} 
		return isInsertable;
	}
	
	public Transaction getTransaction() {
		return transaction;
	}

	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}

	public MTable getTableLeft() {
		return tableLeft;
	}

	public void setTableLeft(MTable tableLeft) {
		this.tableLeft = tableLeft;
	}

	public MTable getTableRight() {
		return tableRight;
	}

	public void setTableRight(MTable tableRight) {
		this.tableRight = tableRight;
	}

	public List<Condition> getAttributesOperatorsValues() {
		return attributesOperatorsValues;
	}

	public void setAttributesOperatorsValues(List<Condition> attributesOperatorsValues) {
		this.attributesOperatorsValues = attributesOperatorsValues;
	}

	
}
