package DBMS.queryProcessing.queryEngine.planEngine.planOperations.updateCommands;

import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.Transaction;



public class UpdateOperation extends AbstractPlanOperation{

	
	private String[] columnsUpdate;
	private String[] columnsValues;
	
	protected void executeOperation(MTable resultTable) throws AcquireLockException {
		
		Transaction transaction = super.getPlan().getTransaction();
		
		boolean isUpdatable = false;
		
		TableScan tableScan = new TableScan(transaction, resultLeft);
		
		int updateTuplesCount = 0;
		
		Tuple tuple = tableScan.nextTuple();
		while(tuple!=null){
			isUpdatable = false;
			
			for (Condition aOv : attributesOperatorsValues) {
				
				if(super.makeComparison(tuple.getColunmData(resultLeft.getIdColumn(aOv.getAtribute())), aOv.getOperator(), aOv.getValue())){
					isUpdatable = true;
				}else{
					isUpdatable = false;
					break;
				}
				
			}
			if(isUpdatable){
				
				String [] data = tuple.getData().clone();
				
				for (int i = 0; i < columnsUpdate.length; i++) {
				
					data[resultLeft.getIdColumn(columnsUpdate[i])] = columnsValues[i];
				}
			

				if (resultLeft.updateTuple(transaction, data, tuple.getTupleID())){
					updateTuplesCount++;
				}else{
					Kernel.log(this.getClass(),"Update error",Level.SEVERE);					
				}
				
			
			}
	
			tuple = tableScan.nextTuple();
			
		}
		
		plan.setOptionalMessage(updateTuplesCount+ " records updated");
		
	}
	
	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	public String getName(){
		
		return "Update"+this.hashCode()+"("+super.resultLeft.getName()+")";
	}

	public String[] getColumnsUpdate() {
		return columnsUpdate;
	}

	public void setColumnsUpdate(String[] columnsUpdate) {
		this.columnsUpdate = columnsUpdate;
	}

	public String[] getColumnsValues() {
		return columnsValues;
	}

	public void setColumnsValues(String[] columnsValues) {
		this.columnsValues = columnsValues;
	}
	
	
	
}