package DBMS.queryProcessing.queryEngine.planEngine.planOperations.deleteCommands;

import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.ITransaction;

public class DeleteOperation extends AbstractPlanOperation {
	

	
	protected void executeOperation(ITable resultTable) {
		
		ITransaction transaction = super.getPlan().getTransaction();
		
		boolean deletable = false;
		
		TableScan tableScan = new TableScan(transaction, resultLeft);
		
		int deleteTuplesCount = 0;
		
		ITuple tuple = tableScan.nextTuple();
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
				ObjectDatabaseId obj = new ObjectDatabaseId(
						String.valueOf(resultLeft.getSchemaManipulate().getId()), 
						String.valueOf(resultLeft.getTableID()), 
						String.valueOf(tableScan.getAtualBlock()), 
						String.valueOf(tuple.getId()));
				if (resultLeft.deleteTuple(transaction, obj) != null ){
					deleteTuplesCount++;
				}else{
					Kernel.log(this.getClass(),"Delete block error",Level.SEVERE);					
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
