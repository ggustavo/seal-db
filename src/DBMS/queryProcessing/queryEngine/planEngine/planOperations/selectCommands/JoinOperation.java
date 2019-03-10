package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.AbstractJoinAlgorithm;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.BlockNestedLoopJoin;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class JoinOperation extends AbstractPlanOperation{

	
	
	private AbstractJoinAlgorithm joinAlgorithm;

	protected void executeOperation(ITable resultTable) {
		if(joinAlgorithm==null){
		//	LogError.save(this.getClass(),"[ERR0] No algorithm was informed");
			joinAlgorithm = new BlockNestedLoopJoin();
		}
		joinAlgorithm.setTransaction(plan.getTransaction());
		joinAlgorithm.setTableLeft(resultLeft);
		joinAlgorithm.setTableRight(resultRight);
		joinAlgorithm.setAttributesOperatorsValues(attributesOperatorsValues);
		if(joinAlgorithm.getiJoinAlgotithmListener()!=null)joinAlgorithm.getiJoinAlgotithmListener().initialize();
		joinAlgorithm.execute(resultTable);
	}
	
	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		JoinOperation ap = (JoinOperation) super.copy(plan,father);
		ap.setJoinAlgorithm(joinAlgorithm);
		return ap;
	
	}
	



	public AbstractJoinAlgorithm getJoinAlgorithm() {
		return joinAlgorithm;
	}

	public void setJoinAlgorithm(AbstractJoinAlgorithm joinAlgorithm) {
		this.joinAlgorithm = joinAlgorithm;
	}
	public Column[] getResultTupleStruct(){
		
		return joinVector(resultLeft.getColumns(), resultRight.getColumns());
		
	}
	
}
