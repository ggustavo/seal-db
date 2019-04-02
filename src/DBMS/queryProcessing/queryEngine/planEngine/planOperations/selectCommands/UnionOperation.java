package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.Transaction;

public class UnionOperation extends AbstractPlanOperation{
	
	protected void executeOperation(MTable resultTable) throws AcquireLockException {
		
		if(resultLeft.getColumnNames().length != resultRight.getColumnNames().length){
			
			throw new IllegalArgumentException("[ERR0] UNION must have the same number of columns");
			
		}
		
		Transaction transaction = super.getPlan().getTransaction();

		TableScan tableScan = new TableScan(transaction, resultLeft);

		Tuple tuple = tableScan.nextTuple();

		while (tuple != null) {
			
			resultTable.writeTuple(transaction,tuple.getStringData());
			tuple = tableScan.nextTuple();

		}
		
		tableScan = new TableScan(transaction, resultRight);
		
		tuple = tableScan.nextTuple();
		
		while (tuple != null) {
			
			resultTable.writeTuple(transaction,tuple.getStringData());
			tuple = tableScan.nextTuple();

		}
		
	}
	
	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		UnionOperation ap = (UnionOperation) super.copy(plan,father);
		return ap;
	
	}



	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	@Override
	public String[] getPossiblesColumnNames() {
	
		return left.getPossiblesColumnNames();
	}
	
}
