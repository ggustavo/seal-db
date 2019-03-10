package DBMS.queryProcessing.queryEngine.planEngine.planOperations.updateCommands;

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



public class UpdateOperation extends AbstractPlanOperation{

	
	private String[] columnsUpdate;
	private String[] columnsValues;
	
	protected void executeOperation(ITable resultTable) {
		
		ITransaction transaction = super.getPlan().getTransaction();
		
		boolean isUpdatable = false;
		
		TableScan tableScan = new TableScan(transaction, resultLeft);
		
		int updateTuplesCount = 0;
		
		ITuple tuple = tableScan.nextTuple();
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
				
				String [] data = tuple.getData();
				for (int i = 0; i < columnsUpdate.length; i++) {
				
					data[resultLeft.getIdColumn(columnsUpdate[i])] = columnsValues[i];
				}
				tuple.setData(data);
				
				ObjectDatabaseId obj = new ObjectDatabaseId(
						String.valueOf(resultLeft.getSchemaManipulate().getId()), 
						String.valueOf(resultLeft.getTableID()), 
						String.valueOf(tableScan.getAtualBlock()), 
						String.valueOf(tuple.getId()));
				if (resultLeft.updateTuple(transaction, tuple, obj) != null){
					updateTuplesCount++;
				}else{
					Kernel.log(this.getClass(),"Update exceeded block size",Level.SEVERE);					
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