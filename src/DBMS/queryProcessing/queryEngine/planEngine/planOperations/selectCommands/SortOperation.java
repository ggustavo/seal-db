package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.Transaction;

public class SortOperation extends AbstractPlanOperation {

	private List<String> columnSorted = null;
	private boolean order = true;


	protected void executeOperation(MTable resultTable) throws AcquireLockException {

		Transaction transaction = super.getPlan().getTransaction();

		TableScan tableScan = new TableScan(transaction, resultLeft);

		int size = resultLeft.getNumberOfTuples(transaction);

		Tuple[] tupleArray = new Tuple[size];
		int count = 0;
		Tuple tuple = tableScan.nextTuple();
		while (tuple != null) {
			if(count < size) {
				tupleArray[count] = tuple;
				count++;
			}
			tuple = tableScan.nextTuple();

		}
		
		if(count>0)Arrays.sort(tupleArray, new Comparator<Tuple>() {

			@Override
			public int compare(Tuple t1, Tuple t2) {

				return compareTuples(t1, t2);
			}

		});
		
		if(count>0) {
			for (int i = 0; i < tupleArray.length; i++) {
				resultTable.writeTuple(transaction,tupleArray[i].getStringData());
			}
		}
		

	}


	public int compareTuples(Tuple t1, Tuple t2) {

		for (String c : columnSorted) {
			int column = resultLeft.getIdColumn(c);

			String data1 = t1.getColunmData(column);
			String data2 = t2.getColunmData(column);

			if (makeComparison(data1, ">", data2)) {
				return order ? 1 : -1;
			} else if (makeComparison(data1, "<", data2)) {
				return order ? -1 : 1;
			}
		}

		return 0;
	}

//	
	public AbstractPlanOperation copy(Plan plan, AbstractPlanOperation father) {
		SortOperation ap = (SortOperation) super.copy(plan, father);
		ap.setOrder(order);
		ap.setColumnSorted(columnSorted);
		return ap;
	}

//	
//	
	public Column[] getResultTupleStruct() {
		return resultLeft.getColumns();
	}

	public List<String> getColumnSorted() {

		return columnSorted;
	}

	public void setColumnSorted(List<String> columnSorted) {
		this.columnSorted = columnSorted;
	}

	public void setColumnSorted(String[] columnSorted) {
		this.columnSorted = Arrays.asList(columnSorted);
	}

	public boolean isOrder() {
		return order;
	}

	public void setOrder(boolean order) {
		this.order = order;
	}

}
