package DBMS.queryProcessing.parse;

import DBMS.queryProcessing.queryEngine.Plan;

public class OptimizerInterceptor {
	
	
	public Plan optimizerPlan(Plan plan){
		
		//LogError.save(this.getClass(), plan.getRoot().toString() );
		
		return plan;
	}
	
	
}
