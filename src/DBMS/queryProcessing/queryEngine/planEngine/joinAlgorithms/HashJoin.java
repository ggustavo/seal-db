package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;

import java.awt.Color;
import java.math.BigInteger;

import java.util.HashMap;

import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;


public class HashJoin extends AbstractJoinAlgorithm{
	
	private int numberOfBuckets;

public void execute(ITable resultTable) {
		
		TableScan tableScanLeft = new TableScan(transaction, tableLeft); // tabela r
		TableScan tableScanRight = new TableScan(transaction, tableRight); // tabela s

		BlockScan blockLeft = null;
		BlockScan blockRight = null;
		ITuple tupleRight = null;
		ITuple tupleLeft = null;
				
		tableScanLeft.reset();
		tableScanRight.reset();
		
		blockLeft = tableScanLeft.nextBlock();
		blockRight = tableScanRight.nextBlock() ;
		
		if (blockLeft != null && blockRight != null){
			showBlockLeft(blockLeft);
			showBlockRight(blockRight);
			tupleRight = blockRight.nextTuple();
			tupleLeft = blockLeft.nextTuple();
		}

		numberOfBuckets = Math.max(1, Math.max(tableLeft.getNumberOfBlocks(transaction), tableRight.getNumberOfBlocks(transaction)));
		
		HashMap<String, Bucket> bucketMapLeft = new HashMap<String, Bucket>();
		HashMap<String, Bucket> bucketMapRight = new HashMap<String, Bucket>();
		
		int columnJoinLeft = tableLeft.getIdColumn(attributesOperatorsValues.get(0).getAtribute());
		int columnJoinRight = tableRight.getIdColumn(attributesOperatorsValues.get(0).getValue());
		String operator = attributesOperatorsValues.get(0).getOperator();
		

		
		//particionamento left-side
			while ( tupleLeft != null) { 

				showTupleLeft(tupleLeft);
				
				String hashID = hashString(tupleLeft.getColunmData(columnJoinLeft));
				
				if(bucketMapLeft.containsKey(hashID)){
					
					Bucket bucket = bucketMapLeft.get(hashID);
					bucket.add(tupleLeft);
					showMatch(tupleLeft, bucket, Color.ORANGE);
				}else{
					Bucket bucket = new Bucket(hashID,transaction,tableLeft.getColumns()); 
					bucket.add(tupleLeft);
					bucketMapLeft.put(hashID, bucket); 
					showBucketLeft(bucket, numberOfBuckets, 40, hashID);
					showMatch(tupleLeft, bucket, Color.ORANGE);
				}
				
				
				tupleLeft = blockLeft.nextTuple();
				if(tupleLeft==null){
					eraseLeft();
					blockLeft = tableScanLeft.nextBlock();
					if(blockLeft!=null){
						showBlockLeft(blockLeft);
						tupleLeft = blockLeft.nextTuple();						
					}
				}
			}

			
	
			
			//particionamento right side
			while (tupleRight != null) {

				showTupleRight(tupleRight);
				
				String hashID = hashString(tupleRight.getColunmData(columnJoinRight));
				
				if(bucketMapRight.containsKey(hashID)){
					
					Bucket bucket = bucketMapRight.get(hashID);
					bucket.add(tupleRight);
					showMatch(tupleRight, bucket, Color.ORANGE);
				}else{
					Bucket bucket = new Bucket(hashID,transaction,tableRight.getColumns()); 
					bucket.add(tupleRight);
					bucketMapRight.put(hashID, bucket); 
					showBucketRight(bucket, numberOfBuckets, 40, hashID);
					showMatch(tupleRight, bucket, Color.ORANGE);				
				}

				tupleRight = blockRight.nextTuple();
				if (tupleRight == null) {
					eraseRight();
					blockRight = tableScanRight.nextBlock();
					if (blockRight != null) {
						showBlockRight(blockRight);
						tupleRight = blockRight.nextTuple();
					}
				}

			}
		
			
			
			
		if(operator.equals("==") || operator.equals("=")){
			
			
			for (Object key : bucketMapLeft.keySet()) {
				if (bucketMapLeft.containsKey(key) && bucketMapRight.containsKey(key)) {
					
					Bucket bucketLeft = bucketMapLeft.get(key);
					Bucket bucketRight = bucketMapRight.get(key);
					
					TableScan bucketLeftScan = new TableScan(transaction,bucketLeft.getTable() );
					TableScan bucketRightScan = new TableScan(transaction,bucketRight.getTable() );
					
					
					while( (tupleLeft = bucketLeftScan.nextTuple())!=null ){
						
						bucketRightScan.reset();
						
						while((tupleRight = bucketRightScan.nextTuple())!=null){
								
							if (match(tupleLeft, tupleRight)) {
								
								resultTable.writeTuple(transaction, tupleLeft.getStringData() + tupleRight.getStringData());
							
								showMatch(bucketLeft, bucketRight, Color.GREEN);
							}
						
						}
						
					}
					
						

				}

			}
			
		}else{
			
			for (Object key : bucketMapLeft.keySet()) {
				
				for (Object key2 : bucketMapRight.keySet()) {
					
					if (bucketMapLeft.containsKey(key) && bucketMapRight.containsKey(key2)) {
						
						
						
						Bucket bucketLeft = bucketMapLeft.get(key);
						Bucket bucketRight = bucketMapRight.get(key2);

						
						TableScan bucketLeftScan = new TableScan(transaction,bucketLeft.getTable() );
						TableScan bucketRightScan = new TableScan(transaction,bucketRight.getTable() );
						
						
						while( (tupleLeft = bucketLeftScan.nextTuple())!=null ){
							
							bucketRightScan.reset();
							
							while((tupleRight = bucketRightScan.nextTuple())!=null){
									
								if (match(tupleLeft, tupleRight)) {
									
									resultTable.writeTuple(transaction, tupleLeft.getStringData() + tupleRight.getStringData());
								
									showMatch(bucketLeft, bucketRight, Color.GREEN);
								}
							
							}
							
						}
					

					}	

				}
				

			}
			
			
			
		}
		
		
	
			
			
	}
	
	

	
	

	private boolean isNumeric(String number) {
		try {
			Double.parseDouble(number);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	private String hashString(String s) {
		String str = "";

		if (isNumeric(s)) {
			BigInteger num = new BigInteger(s);
			return num.mod(new BigInteger(numberOfBuckets + "")).add(BigInteger.ONE).toString();
		}

		for (int i = 0; i < s.length(); i++) {
			str = str + ((int) s.charAt(i));
		}
		BigInteger num = new BigInteger(str);
		return num.mod(new BigInteger(numberOfBuckets + "")).add(BigInteger.ONE).toString();
	}	
	


}
