package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.ITransaction;


public class ProjectionOperation extends AbstractPlanOperation {

	
	private String[] attributesProjected;
	private List<Integer> dataIDs;
	private Column[] projectedColumns;
	
	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		ProjectionOperation ap = (ProjectionOperation) super.copy(plan,father);
		String[] newAttributesProjected = null;
		if(attributesProjected!=null){
			newAttributesProjected = new String[attributesProjected.length];
			for (int i = 0; i < attributesProjected.length; i++) {				
				newAttributesProjected[i] = new String(attributesProjected[i]);
			}
			
		}
		ap.setAttributesProjected(newAttributesProjected);
		return ap;
	
	}
	
	public void createNewTableHeader() {

		String[] tupleStructures = resultLeft.getColumnNames();
		dataIDs = new ArrayList<Integer>();
		
		List<Column> cols = new LinkedList<>();
		
		for (int i = 0; i < attributesProjected.length; i++) {

			for (int j = 0; j < tupleStructures.length; j++) {
			
				if (attributesProjected[i].equals(tupleStructures[j])) {	
					
					cols.add(new Column(tupleStructures[j],"varchar"));
					dataIDs.add(j);
					break;
				}

			}

		}
		
		projectedColumns = cols.toArray(new Column[cols.size()]);
		
		//LogError.save(this.getClass(),"NEWH " + newHeader.toString());
	}
	
	public String projectTuple(ITuple tuple){
		StringBuffer tupleString = new StringBuffer();
		for (Integer id : dataIDs) {
			tupleString.append(tuple.getColunmData(id));
			tupleString.append("|");
		}
		return tupleString.toString();
	}
	
	
	
	protected void executeOperation(ITable resultTable) {
		if (dataIDs.isEmpty())Kernel.log(this.getClass(),"dataIDs is Empty ",Level.SEVERE);
		ITransaction transaction = super.getPlan().getTransaction();

		TableScan tableScan = new TableScan(transaction, resultLeft);

		ITuple tuple = tableScan.nextTuple();

		while (tuple != null) {
			
			resultTable.writeTuple(transaction, projectTuple(tuple));
			tuple = tableScan.nextTuple();

		}

	}
	
	
	
	
	
	public String[] getAttributesProjected() {
		return attributesProjected;
	}

	public void setAttributesProjected(String[] attributesProjected) {
		this.attributesProjected = attributesProjected;
	}

	
	public Column[] getResultTupleStruct() {
		createNewTableHeader();
		return projectedColumns;
	}
	
	public String[] getPossiblesColumnNames() {
		if(attributesProjected==null) {
			return super.getPossiblesColumnNames();
		}
		return attributesProjected;
	}
}
