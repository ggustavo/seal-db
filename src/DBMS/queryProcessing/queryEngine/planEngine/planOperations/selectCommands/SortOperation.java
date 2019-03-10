package DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.Bucket;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;





public class SortOperation extends AbstractPlanOperation {


	private List<String> columnSorted = null;
	private boolean order = true;
	
	private static long count = 0;
	
	protected void executeOperation(ITable resultTable) {
		
		List<Bucket> buckets = sortFase();
		
		if(buckets != null && !buckets.isEmpty()) {
			
			Bucket bucket = mergeFase(buckets);
			
			if(bucket != null) {
				TableScan bucketScan = new TableScan(plan.getTransaction(),bucket.getTable() );
				ITuple tuple = null;
				while( (tuple = bucketScan.nextTuple())!=null ){
					resultTable.writeTuple(plan.getTransaction(), tuple.getStringData());
				}			
			}
			
		}
		
		
		
	}

	
	private int BLOCKS_PER_BUCKET = 100;
	
	public List<Bucket> sortFase(){
		List<Bucket> buckets = new ArrayList<>();
		
		TableScan scan = new TableScan(plan.getTransaction(), resultLeft); 
		
		BlockScan blockScan = scan.nextBlock();
		ITuple tuple = null;
		
		int count = 0;
		
		List<ITuple> tempTuples = new LinkedList<ITuple>();
		
		int currentID;
		
		while(blockScan != null){
			
			currentID = blockScan.getAtualBlock();
			
			while((tuple = blockScan.nextTuple())!=null){
				
				tempTuples.add(tuple);
				tuple = blockScan.nextTuple();
			}
			
			count++;
			
			if(!tempTuples.isEmpty() && count == BLOCKS_PER_BUCKET){
				
				createSortedBucket(buckets, currentID, tempTuples);
				
				tempTuples.clear();
				count = 0;
			}
			
			blockScan = scan.nextBlock();
			
			if(blockScan == null && count > 0) {
				createSortedBucket(buckets, currentID, tempTuples);
			}
		}
		return buckets;
	}



	private void createSortedBucket(List<Bucket> buckets, int id, List<ITuple> tempTuples) {
		Bucket bucket = new Bucket(id+"", plan.getTransaction(),resultLeft.getColumns());
		buckets.add(bucket);	
		
		if(tempTuples.size() == 1){
			bucket.add(tempTuples.get(0));
			
		}else{
			ITuple[] tupleArray = new ITuple[tempTuples.size()];
			Arrays.sort(tempTuples.toArray(tupleArray), new Comparator<ITuple>() {

				@Override
				public int compare(ITuple t1, ITuple t2) {
					
					return compareTuples(t1, t2);
				}
				
				
			} );
			for (ITuple t : tupleArray) {
				bucket.add(t);
			}
		}
	}
	
	
	
	public Bucket mergeFase(List<Bucket> buckets){
		
		
		if(buckets.size() == 1){
		
			return buckets.get(0);
		}
		
		List<Bucket> faseList = new ArrayList<>();
		
		int size = buckets.size() % 2 == 0 ? buckets.size() : buckets.size() - 1;
		
		for (int i = 0; i < size; i+=2) {
			Bucket b1 = buckets.get(i); 
			Bucket b2 = buckets.get(i+1);
			faseList.add(merge(b1, b2));
		}
		
		if(buckets.size() % 2 != 0 ){
			faseList.add(buckets.get(buckets.size()-1));
		}
		
		return mergeFase(faseList);
	}
	
	
	
	public Bucket merge(Bucket b1, Bucket b2){
		//Bucket newBucket = new Bucket(b1.getId()+"_"+b2.getId(), plan.getTransaction(),resultLeft.getColumns());
		Bucket newBucket = new Bucket("t"+plan.getTransaction().getIdT()+"bucket_"+(count++), plan.getTransaction(),resultLeft.getColumns());
	
		TableScan scanB1 = new TableScan(plan.getTransaction(), b1.getTable()); 
		TableScan scanB2 = new TableScan(plan.getTransaction(), b2.getTable()); 
		
		ITuple tB1 = scanB1.nextTuple();
		ITuple tB2 = scanB2.nextTuple();
		
		while(tB1 != null && tB2 != null){
			
			if(compareTuples(tB1, tB2) <= 0){
				newBucket.add(tB1);
				tB1 = scanB1.nextTuple();
			}else{
				newBucket.add(tB2);
				tB2 = scanB2.nextTuple();
			}
			
		}
		
		while(tB1 != null){
			newBucket.add(tB1);
			tB1 = scanB1.nextTuple();
		}
		
		while(tB2 != null){
			newBucket.add(tB2);
			tB2 = scanB2.nextTuple();
		}
		
		return newBucket;
	}
	
	
	public int compareTuples(ITuple t1, ITuple t2) {
		
		for (String c : columnSorted) {
			int column = resultLeft.getIdColumn(c);
			
			String data1 = t1.getColunmData(column);
			String data2 = t2.getColunmData(column);
			
			if(makeComparison(data1, ">", data2)){
				return order ? 1 : -1;
			}else if(makeComparison(data1, "<", data2)){
				return order ? -1 : 1;
			}
		}
		
		return 0;
	}
	
	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		SortOperation ap = (SortOperation) super.copy(plan,father);
		ap.setOrder(order);
		ap.setColumnSorted(columnSorted);
		return ap;
	}
	
	
	public Column[] getResultTupleStruct(){
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
