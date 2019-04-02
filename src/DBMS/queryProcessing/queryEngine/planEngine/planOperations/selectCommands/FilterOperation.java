package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;



import java.util.LinkedList;
import java.util.List;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.MultiResultOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.Transaction;



public class FilterOperation extends AbstractPlanOperation{
			
	private int targetPosition;
	
	public static final String NONE = ".none.";

	protected void executeOperation(MTable resultTable) throws AcquireLockException {
		
		List<MTable> results = getResults();
		//LogError.save(this.getClass(),results.isEmpty());
		
		MTable target = getForeache().getResultTable();
		//LogError.save(this.getClass(),target + " <<<=");
		
//		for (PointerTable PointerTable : results) {
//			LogError.save(this.getClass(),"-> " +PointerTable.getName());
//		}
		
//	
//		for (AbstractPlanOperation a : getOperationsResults()) {
//			LogError.save(this.getClass(),"-- " +a.getName());
//		}
		
	
		makeGroups();
		
//		for (List<Condition> group : orGroups) {
//			LogError.save(this.getClass(),"Group OR");
//			for (Condition attributeOperatorValueStructure : group) {
//				LogError.save(this.getClass(),attributeOperatorValueStructure + " " +attributeOperatorValueStructure.getExternalOperator());
//			}
//			LogError.save(this.getClass(),);
//		}
		
		Transaction transaction = super.getPlan().getTransaction();
		
		TableScan tableScan = new TableScan(transaction, target);
		boolean isInsertable = false;
		Tuple tuple = tableScan.nextTuple();
		while(tuple!=null){
			
			isInsertable = false;
			
			for (List<Condition> group : orGroups) {
				
				for (Condition c : group) {
					
					if(c.getType() == Condition.COLUMN_COLUMN) {
						if(super.makeComparison(
								tuple.getColunmData(target.getIdColumn(c.getAtribute())), 
								c.getOperator(), 
								tuple.getColunmData(target.getIdColumn(c.getValue())))){
							isInsertable = true;
						}else {
							isInsertable = false;
							break;
						}
					}
					
					if(c.getType() == Condition.COLUMN_VALUE) {
						if(super.makeComparison(
								tuple.getColunmData(target.getIdColumn(c.getAtribute())), 
								c.getOperator(), 
								c.getValue())){
							isInsertable = true;
						}else {
							isInsertable = false;
							break;
						}
					}
					
					if(c.getType() == Condition.CORRELATION_EXISTS || c.getType() == Condition.CORRELATION_NOT_EXISTS) {
						MTable r = results.get(Integer.parseInt(c.getTable2()));
						boolean exists = false;
						
						if(c.getAtribute() == null || c.getAtribute().equals(NONE)) {
							exists = existsNoCodiction(transaction, r);
						}else {
							exists = existsCodiction(transaction, tuple,target,c,r);
						}
						
						if(c.getType() == Condition.CORRELATION_EXISTS ) {
							if(exists)isInsertable = true;
						}
						if(c.getType() == Condition.CORRELATION_NOT_EXISTS ) {
							if(!exists)isInsertable = true;
						}
							
					}
					
					
				}
				//End Group
				if(isInsertable)break;				
			}
			
			
			if(isInsertable) {
				resultTable.writeTuple(transaction,tuple.getStringData());
			}
			
			tuple = tableScan.nextTuple();
			
		}
		
		
	}
	
	private boolean existsNoCodiction(Transaction transaction, MTable table) throws AcquireLockException {
		TableScan tr = new TableScan(transaction, table);
		Tuple tuple = tr.nextTuple();
		return tuple != null; 
	}
	
	private boolean existsCodiction(Transaction transaction, Tuple tupleTarget,MTable target,Condition c,MTable table) throws AcquireLockException {
		
		
		TableScan tr = new TableScan(transaction, table);
		Tuple tuple = tr.nextTuple();
			
			boolean find = false;
			
			String operator = c.getOperator();
			if(operator.equalsIgnoreCase("in") || operator.equalsIgnoreCase("not in")) {
				operator = "==";
			}
			
			while(tuple != null) {
				
				if(super.makeComparison(
						tupleTarget.getColunmData(target.getIdColumn(c.getAtribute())), 
						operator, 
						tuple.getColunmData(table.getIdColumn(c.getValue())))){
					find = true;
				}
				
				tuple = tr.nextTuple();
			}	
			
			if(c.getOperator().equalsIgnoreCase("in") ) {
				if(find)return true;
			}else
			if(c.getOperator().equalsIgnoreCase("not in") ) {
				if(!find)return true;
			}else {
				if(find)return true;
			}
					
		
		
		return false; 
	}
	
	private List<List<Condition>> orGroups;
	
	private void makeGroups() {
		orGroups = new LinkedList<>();
		
		List<Condition> group = null;
		
		for (int i = 0; i < getAttributesOperatorsValues().size(); i++) {
			
			Condition aov = getAttributesOperatorsValues().get(i);
			
			if(group == null) {
				group = new LinkedList<>();
				orGroups.add(group);
			}
			group.add(aov);
		
			if(aov.getExternalOperator().equalsIgnoreCase("or")) {
				group = null;
			
			}
			
		}
		
	}
	

	
	public Column[] getResultTupleStruct(){
		AbstractPlanOperation foreache = getForeache();
		if(foreache!=null)return foreache.getResultTupleStruct();
		return null;
	}
	

	
	public String[] getPossiblesColumnNames(){
		AbstractPlanOperation foreache = getForeache();
		if(foreache!=null)return foreache.getPossiblesColumnNames();
		return new String[0];
		
	}

	
	
	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		FilterOperation filter = (FilterOperation) super.copy(plan,father);
		filter.setTargetPosition(targetPosition);
		return filter;
	}

	public List<MTable> getResults() {
		return MultiResultOperation.getResults(left);
	}


	public List<AbstractPlanOperation> getOperationsResults() {
		return MultiResultOperation.getOperationsResults(left);
	}

	public AbstractPlanOperation getForeache() {
		for (int i = 0; i < getOperationsResults().size(); i++) {
			if(i == targetPosition) {
				return getOperationsResults().get(i);
			}
		} 
		return null;
	}

	public int getTargetPosition() {
		return targetPosition;
	}



	public void setTargetPosition(int targetPosition) {
		this.targetPosition = targetPosition;
	}

	
	
	
}
