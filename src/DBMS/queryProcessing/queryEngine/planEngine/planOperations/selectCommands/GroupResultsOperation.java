package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import java.util.List;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.planEngine.MultiResultOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;


public class GroupResultsOperation extends AbstractPlanOperation implements MultiResultOperation{
						

	protected void executeOperation(ITable resultTable) {
		
		//LogError.save(this.getClass(),getResults());
	}
	
	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	
	public String[] getPossiblesColumnNames(){
		if(resultLeft==null)return null;
		return resultLeft.getColumnNames();
		
	}

	public List<ITable> getResults() {
		return MultiResultOperation.getResults(this);
	}

	public List<AbstractPlanOperation> getOperationsResults() {
		return MultiResultOperation.getOperationsResults(this);
	}

	
}
