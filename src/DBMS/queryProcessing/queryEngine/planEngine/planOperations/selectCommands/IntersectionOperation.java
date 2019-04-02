package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;


import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.Transaction;

public class IntersectionOperation extends AbstractPlanOperation{
	
	protected void executeOperation(MTable resultTable) throws AcquireLockException {
		
		if(resultLeft.getColumnNames().length != resultRight.getColumnNames().length){
			
			throw new IllegalArgumentException("[ERR0] UNION must have the same number of columns");
			
		}
		
		
		Transaction transaction = super.getPlan().getTransaction();

		TableScan scanLeft = new TableScan(transaction, resultLeft);
		
		TableScan scanRight = new TableScan(transaction, resultRight);
		
		
		scanLeft.reset();
		scanRight.reset();
		
		
		Tuple tupleLeft = scanLeft.nextTuple();
		Tuple tupleRight = scanRight.nextTuple();
		
		
		while (tupleLeft != null) {
			
			scanRight.reset();
			tupleRight = scanRight.nextTuple();
			
			while (tupleRight != null) {
				
				//Kernel.info(this.getClass(),tupleLeft.getStringData() + " - " + tupleRight.getStringData());
				if (tupleLeft.getStringData().equals(tupleRight.getStringData())) {

					resultTable.writeTuple(transaction, tupleLeft.getStringData());

				}

				tupleRight = scanRight.nextTuple();
			}

			tupleLeft = scanLeft.nextTuple();
		}
		
	}
	
	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		IntersectionOperation ap = (IntersectionOperation) super.copy(plan,father);
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
