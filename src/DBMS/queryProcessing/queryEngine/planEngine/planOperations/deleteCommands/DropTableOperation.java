package DBMS.queryProcessing.queryEngine.planEngine.planOperations.deleteCommands;

import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.Transaction;

public class DropTableOperation extends AbstractPlanOperation {
	

	protected void executeOperation(MTable resultTable) throws AcquireLockException {
		
		Transaction transaction = super.getPlan().getTransaction();
		
		TableScan tableScan = new TableScan(transaction, resultLeft);
	
		Tuple tuple = tableScan.nextTuple();
		while (tuple != null) {

	
			resultLeft.deleteTuple(transaction, tuple.getTupleID());

			tuple = tableScan.nextTuple();

		}
	//	Kernel.info(this.getClass(),resultLeft.getName() + " " + resultLeft.getTableID());
		//Kernel.info(this.getClass(),resultLeft.getSchemaManipulate().getTables().toString());	
		
		MTable table = resultLeft.getSchemaManipulate().removeTable(resultLeft.getName());
		//Kernel.info(this.getClass(),resultLeft.getSchemaManipulate().getTables());	
		
		//Kernel.info(this.getClass(),table.getPath());
		//Kernel.removeFile(table.getPath());
		
		Kernel.getInitializer().removeTable(table);
		
		
		plan.setOptionalMessage(table.getName() + " table removed");
		Kernel.log(this.getClass(),table.getName() + " table removed",Level.WARNING);	
		//Kernel.info(this.getClass(),table.getSchemaManipulate().getTables());	
		
		}
	
	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	public String getName(){
		
		return "DropTable"+this.hashCode()+"("+super.resultLeft.getName()+")";
	}

	
}
