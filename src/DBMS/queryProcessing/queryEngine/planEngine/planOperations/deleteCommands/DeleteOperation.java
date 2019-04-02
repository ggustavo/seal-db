package DBMS.queryProcessing.queryEngine.planEngine.planOperations.deleteCommands;

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

public class DeleteOperation extends AbstractPlanOperation {
	

	
	protected void executeOperation(MTable resultTable) throws AcquireLockException {
		
		Transaction transaction = super.getPlan().getTransaction();
		
		boolean deletable = false;
		
		TableScan tableScan = new TableScan(transaction, resultLeft);
		
		int deleteTuplesCount = 0;
		
		Tuple tuple = tableScan.nextTuple();
		while(tuple!=null){
			deletable = false;
			
			for (Condition aOv : attributesOperatorsValues) {
				
				if(super.makeComparison(tuple.getColunmData(resultLeft.getIdColumn(aOv.getAtribute())), aOv.getOperator(), aOv.getValue())){
					deletable = true;
				}else{
					deletable = false;
					break;
				}
				
			}
			if(deletable){
				if (resultLeft.deleteTuple(transaction, tuple.getTupleID()) ){
					deleteTuplesCount++;
				}else{
					Kernel.log(this.getClass(),"Delete error",Level.SEVERE);					
				}
			}
	
			tuple = tableScan.nextTuple();
			
		}
		
		plan.setOptionalMessage(deleteTuplesCount+ " records deleted");
		
	}
	
	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	public String getName(){
		
		return "Delete"+this.hashCode()+"("+super.resultLeft.getName()+")";
	}

	
}
