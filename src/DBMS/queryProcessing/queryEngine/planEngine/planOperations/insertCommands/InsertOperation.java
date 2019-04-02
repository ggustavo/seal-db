package DBMS.queryProcessing.queryEngine.planEngine.planOperations.insertCommands;

import java.util.LinkedList;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class InsertOperation extends AbstractPlanOperation{


	private String[] columns;
	private LinkedList<String[]> values;
	
	@Override
	protected void executeOperation(MTable resultTable) throws AcquireLockException {
		MTable table = resultLeft;
		
	
		for (String[] v : values) {
			
			
			String data[] = new String[table.getColumnNames().length];
			
			for (int i = 0; i < v.length; i++) {
				int index = table.getIdColumn(columns[i]);
				data[index] = v[i];
			}
		
	
			table.writeTuple(plan.getTransaction(), MTable.arrayToString(data));
		
		}
		plan.setOptionalMessage(values.size()+ " records inserted");
	}
	

	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	public String getName(){
		
		return "Insert"+this.hashCode()+"("+super.resultLeft.getName()+")";
	}


	public String[] getColumns() {
		return columns;
	}


	public void setColumns(String[] columns) {
		this.columns = columns;
	}


	public LinkedList<String[]> getValues() {
		return values;
	}


	public void setValues(LinkedList<String[]> values) {
		this.values = values;
	}

}
