package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.MultiResultOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;



public class SubplanOperation extends AbstractPlanOperation implements MultiResultOperation{
	
	
	private Plan subplan;
	
	protected void executeOperation(ITable resultTable) {
		
	}
	
	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		SubplanOperation ap = (SubplanOperation) super.copy(plan,father);
		ap.setSubplan(subplan);
		return ap;
	
	}
	

	public String getName(){
		if(subplan==null){
			Kernel.log(this.getClass(),"Subplan NULL",Level.SEVERE);
			return null;
		}
		return "Subplan"+code+"("+subplan.getRoot().getName()+")";
	}

		
	public Column[] getResultTupleStruct(){
		if(subplan==null){
			Kernel.log(this.getClass(),"Subplan NULL",Level.SEVERE);
			return null;
		}
		return resultLeft.getColumns();
	}
	
	
	
	@Override
	public String[] getPossiblesColumnNames() {
		if(subplan==null){
			Kernel.log(this.getClass(),"Subplan NULL",Level.SEVERE);
			return null;
		}
		return subplan.getRoot().getPossiblesColumnNames();
	}

	public Plan getSubplan() {
		return subplan;
	}

	public void setSubplan(Plan subplan) {
		this.subplan = subplan;
	}

	@Override
	public List<ITable> getResults() {
		if(subplan==null){
			Kernel.log(this.getClass(),"Subplan NULL",Level.SEVERE);
			return null;
		}
		return MultiResultOperation.getResults(subplan.getRoot());
	}
	public List<AbstractPlanOperation> getOperationsResults() {
		if(subplan==null){
			Kernel.log(this.getClass(),"Subplan NULL",Level.SEVERE);
			return null;
		}
		return MultiResultOperation.getOperationsResults(subplan.getRoot());
	}
	
	
}
