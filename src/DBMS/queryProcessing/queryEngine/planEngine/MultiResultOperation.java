package DBMS.queryProcessing.queryEngine.planEngine;

import java.util.LinkedList;
import java.util.List;


import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public interface MultiResultOperation {
	
	List<ITable> getResults();
	List<AbstractPlanOperation> getOperationsResults();
	
	
	public static List<ITable> getResults(AbstractPlanOperation operation){
		List<ITable> results = new LinkedList<ITable>();
			if(operation==null)return results;
			
			
			if(operation.getLeft() != null) {
				if(operation.getLeft() instanceof MultiResultOperation) {
					List<ITable> leftResults = ((MultiResultOperation) operation.getLeft()).getResults();
					if(leftResults!=null)results.addAll(leftResults);
				}else {
					if(operation.getResultLeft() != null)results.add(operation.getResultLeft());					
				}
			}
		
	
			if(operation.getRight()!=null) {
				if(operation.getRight() instanceof MultiResultOperation) {
					List<ITable> rightResults = ((MultiResultOperation) operation.getRight()).getResults();
					if(rightResults!=null)results.addAll(rightResults);					
				}else {
					if(operation.getResultRight() != null) results.add(operation.getResultRight());
				}
				
			}
		
		return results;
	}
	
	public static List<AbstractPlanOperation> getOperationsResults(AbstractPlanOperation operation){
		List<AbstractPlanOperation> operations = new LinkedList<AbstractPlanOperation>();
		if(operation==null)return operations;
		
		if(! (operation instanceof MultiResultOperation) ) {
			operations.add(operation);
			
		}else {
			if(operation.getLeft() != null) {
				
				if(operation.getLeft() instanceof MultiResultOperation) {
					List<AbstractPlanOperation> leftOperations = ((MultiResultOperation) operation.getLeft()).getOperationsResults();
					if(leftOperations!=null)operations.addAll(leftOperations);
				}else {
					operations.add(operation.getLeft());					
				}
			}
		

			if(operation.getRight()!=null) {
				if(operation.getRight() instanceof MultiResultOperation) {
					List<AbstractPlanOperation> rightOperation = ((MultiResultOperation) operation.getRight()).getOperationsResults();
					if(rightOperation!=null)operations.addAll(rightOperation);					
				}else {
					operations.add(operation.getRight());
				}
				
			}
		}
		
		return operations;
	}
}
