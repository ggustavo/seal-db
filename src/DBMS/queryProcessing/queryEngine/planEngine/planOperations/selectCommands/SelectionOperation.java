package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.ITransaction;


public class SelectionOperation extends AbstractPlanOperation{

	
	protected void executeOperation(ITable resultTable) {
		
		ITransaction transaction = super.getPlan().getTransaction();
		
		boolean isInsertable = false;
		
		TableScan tableScan = new TableScan(transaction, resultLeft);
		
		
		ITuple tuple = tableScan.nextTuple();
		while(tuple!=null){
			isInsertable = false;
			
			for (Condition aOv : attributesOperatorsValues) {
				
				if(super.makeComparison(tuple.getColunmData(resultLeft.getIdColumn(aOv.getAtribute())), aOv.getOperator(), aOv.getValue())){
					isInsertable = true;
				}else{
					isInsertable = false;
					break;
				}
				
			}
			if(isInsertable || attributesOperatorsValues.isEmpty()){
				resultTable.writeTuple(transaction,tuple.getStringData());
			}
	
			tuple = tableScan.nextTuple();
			
		}
		
		
	}
	
	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
}
