package DBMS.queryProcessing;

import DBMS.fileManager.Schema;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.AggregationOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.FilterOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.GroupResultsOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SortOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;

public class ExtendedRelationalAlgebraAPI {

	public static TableOperation newTable(Plan plan, Schema schema, String tableName) {
		MTable table = schema.getTableByName(tableName);
		TableOperation tableOP = new TableOperation();
		tableOP.setPlan(plan);
		tableOP.setResultLeft(table);
		return tableOP;
	}

	public static ProjectionOperation newProjection(Plan plan, String... columns) {
		ProjectionOperation projectionOP = new ProjectionOperation();
		projectionOP.setPlan(plan);
		projectionOP.setAttributesProjected(columns);
		return projectionOP;
	}

	public static SelectionOperation newSelection(Plan plan, Condition... conditions) {
		SelectionOperation selectOP = new SelectionOperation();
		selectOP.setPlan(plan);
		for (Condition condition : conditions) {
			selectOP.getAttributesOperatorsValues().add(condition);
		}
		return selectOP;
	}

	public static JoinOperation newJoin(Plan plan, Condition... conditions) {
		JoinOperation joinOP = new JoinOperation();
		joinOP.setPlan(plan);
		for (Condition condition : conditions) {
			joinOP.getAttributesOperatorsValues().add(condition);
		}
		return joinOP;
	}

	public static SortOperation newSort(Plan plan, boolean isASC, String... columns) {
		SortOperation sortOP = new SortOperation();
		sortOP.setPlan(plan);
		sortOP.setColumnSorted(columns);
		sortOP.setOrder(isASC);
		return sortOP;
	}

	public static AggregationOperation newAggregation(Plan plan, Condition... conditions) {
		AggregationOperation aggregationOP = new AggregationOperation();
		aggregationOP.setPlan(plan);
		for (Condition condition : conditions) {
			aggregationOP.getAttributesOperatorsValues().add(condition);
		}
		return aggregationOP;
	}

	public static FilterOperation newFilterOperation(Plan plan, Condition... conditions) {
		FilterOperation filterOP = new FilterOperation();
		filterOP.setPlan(plan);
		for (Condition condition : conditions) {
			filterOP.getAttributesOperatorsValues().add(condition);
		}
		return filterOP;
	}

	public static GroupResultsOperation newGroupResultsOperation(Plan plan, AbstractPlanOperation result0,
			AbstractPlanOperation result1) {
		GroupResultsOperation groupResultsOP = new GroupResultsOperation();
		groupResultsOP.setLeft(result0);
		groupResultsOP.setRight(result1);
		groupResultsOP.setPlan(plan);
		return groupResultsOP;
	}
}
