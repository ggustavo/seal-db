package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import java.util.LinkedList;
import java.util.List;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.ITransaction;

public class AggregationOperation extends AbstractPlanOperation {

	public static final String GROUPING = "group";	
	public static final String AVG = "AVG";
	public static final String COUNT = "COUNT";
	public static final String MAX = "MAX";
	public static final String MIN = "MIN";
	//public static final String STDEV = "STDEV";
	public static final String SUM = "SUM";
	//public static final String VAR = "VAR"; 
	

	private Column[] projectedColumns;
	
	
	protected void executeOperation(ITable resultTable) {

		ITransaction transaction = super.getPlan().getTransaction();

		TableScan tableScan = new TableScan(transaction, resultLeft);

		List<MathAggregation> ma = getMatAggregations();

		ITuple tuple = tableScan.nextTuple();

		ITuple beforeTuple = null;

		while (tuple != null) {

			if (beforeTuple == null) {

				for (MathAggregation m : ma) {
					m.update(tuple);
				}
				beforeTuple = tuple;
			} else {
				if (compareTuples(tuple, beforeTuple)) {

					for (MathAggregation m : ma) {
						m.update(tuple);
					}
					beforeTuple = tuple;

				} else {
					resultTable.writeTuple(transaction, generateTuple(ma));
					beforeTuple = null;
				}

			}
			if (beforeTuple != null)
				tuple = tableScan.nextTuple();

		}
		resultTable.writeTuple(transaction, generateTuple(ma));

	}
	
	public String generateTuple(List<MathAggregation> ma) {
		String tuple = "";
		for (MathAggregation m : ma) {
			tuple+=m.getResult()+"|";
			m.reset();
		}
		
		return tuple;
	}
	
	public boolean compareTuples(ITuple t1, ITuple t2) {
		
		for (Condition c : attributesOperatorsValues) {
			int column = resultLeft.getIdColumn(c.getAtribute());
			
			if(c.getOperator().equals(GROUPING)) {
				
				String data1 = t1.getColunmData(column);
				String data2 = t2.getColunmData(column);
				
				if(makeComparison(data1, "!=", data2)){
					return false;
				}				
			}
		}
		
		return true;
	}


	
	class MathAggregation {
		Condition aov;

		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;
		int count;
		double sum;
		String valueData;

		public MathAggregation(Condition aov) {
			this.aov = aov;
			reset();
		}

		void update(ITuple tuple) {
			count++;
			int column = resultLeft.getIdColumn(aov.getAtribute());
			String data = tuple.getColunmData(column);			
			double value;
			
			switch (aov.getOperator()) {

			case GROUPING:
			//	LogError.save(this.getClass(),data);
				valueData = data;
				return;
			case AVG:
				sum += Double.parseDouble(data);
				return;
			case MAX:
				value = Double.parseDouble(data);
				if (value > max)
					max = value;
				return;
			case MIN:
				value = Double.parseDouble(data);
				if (value < min)
					min = value;
				return;
			case SUM:
				sum += Double.parseDouble(data);
				return;

			default:
				return;
			}
		}

		void reset() {
			valueData = null;
			max = Double.MIN_VALUE;
			min = Double.MAX_VALUE;
			count = 0;
			sum = 0;
		}
		
		public String getResult() {

			switch (aov.getOperator()) {

			case GROUPING:
				return valueData;
			case COUNT:
				return String.valueOf(count);
			case AVG:
				return String.valueOf(sum / count);
			case MAX:
				return String.valueOf(max);
			case MIN:
				return String.valueOf(min);
			case SUM:
				return String.valueOf(sum);

			default:
				return null;
			}
		}
	}
	
	
	private List<MathAggregation> getMatAggregations(){
		List<MathAggregation> list = new LinkedList<>();
		for(Condition aov : attributesOperatorsValues ) {
			MathAggregation m = new MathAggregation(aov);
			list.add(m);
		}
		return list;
	}
	
	
	private void createNewTableHeader() {
		
		projectedColumns = new Column[attributesOperatorsValues.size()];
		
		for(int i= 0;i < attributesOperatorsValues.size(); i++ ) {
			
			Condition aov = attributesOperatorsValues.get(i);
			projectedColumns[i] = new Column(aov.getAtribute(),"varchar");
//			if(aov.getOperator().equals(GROUPING)) {
//				newHeader.append(aov.getAtribute());				
//			}else {
//				newHeader.append(aov.getOperator()+"_"+aov.getAtribute());
//			}

		}
	}
	
	
	public String[] getPossiblesColumnNames() {

		if(attributesOperatorsValues.isEmpty())return super.getPossiblesColumnNames();
		String[] v = new String[attributesOperatorsValues.size()];
		
		for(int i= 0;i < attributesOperatorsValues.size(); i++ ) {
		
			Condition aov = attributesOperatorsValues.get(i);
			
			v[i] = (aov.getAtribute());	
			
//			if(aov.getOperator().equals(GROUPING)) {
//				v[i] = (aov.getAtribute());				
//			}else {
//				v[i] = (aov.getOperator()+"_"+aov.getAtribute());
//			}
			
		}
		return v;
	}

	public Column[] getResultTupleStruct(){
		createNewTableHeader();
		return projectedColumns;
	}

	
}
