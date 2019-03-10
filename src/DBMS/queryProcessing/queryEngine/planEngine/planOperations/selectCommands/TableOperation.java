package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class TableOperation extends AbstractPlanOperation{

	
	

	protected void executeOperation(ITable resultTable) {
	

	}


	public String getName() {
		if(resultLeft==null)return "null";
		return resultLeft.getName();
	}
	
	public String[] getPossiblesColumnNames(){
		if(resultLeft==null)return null;
		return resultLeft.getColumnNames();
		
	}



	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	
	public ITable getResultTable() {
		return resultLeft;
	}

}
