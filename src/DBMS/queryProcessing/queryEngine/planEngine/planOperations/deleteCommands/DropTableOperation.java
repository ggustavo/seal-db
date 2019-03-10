package DBMS.queryProcessing.queryEngine.planEngine.planOperations.deleteCommands;

import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.fileManager.ObjectDatabaseId;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.TableManipulate;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.ITransaction;

public class DropTableOperation extends AbstractPlanOperation {
	

	protected void executeOperation(ITable resultTable) {
		
		ITransaction transaction = super.getPlan().getTransaction();
		
		TableScan tableScan = new TableScan(transaction, resultLeft);
	
		ITuple tuple = tableScan.nextTuple();
		while (tuple != null) {

			ObjectDatabaseId obj = new ObjectDatabaseId(String.valueOf(resultLeft.getSchemaManipulate().getId()),
					String.valueOf(resultLeft.getTableID()), String.valueOf(tableScan.getAtualBlock()),
					String.valueOf(tuple.getId()));
			resultLeft.deleteTuple(transaction, obj);

			tuple = tableScan.nextTuple();

		}
	//	Kernel.info(this.getClass(),resultLeft.getName() + " " + resultLeft.getTableID());
		//Kernel.info(this.getClass(),resultLeft.getSchemaManipulate().getTables().toString());	
		
		ITable table = resultLeft.getSchemaManipulate().removeTable(resultLeft.getName());
		//Kernel.info(this.getClass(),resultLeft.getSchemaManipulate().getTables());	
		if(table.close()){
			
		}
		//Kernel.info(this.getClass(),table.getPath());
		Kernel.removeFile(table.getPath());
		
		Kernel.getCatalagInitializer().removeTable((TableManipulate) table);
		
		
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
