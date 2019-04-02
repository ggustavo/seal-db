package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;

import java.math.BigInteger;

import java.util.HashMap;

import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;


public class HashJoin extends AbstractJoinAlgorithm{
	
	private int numberOfBuckets;

public void execute(MTable resultTable) throws AcquireLockException {
		
		TableScan tableScanLeft = new TableScan(transaction, tableLeft); // tabela r
		TableScan tableScanRight = new TableScan(transaction, tableRight); // tabela s


		Tuple tupleRight = null;
		Tuple tupleLeft = null;
				
		tableScanLeft.reset();
		tableScanRight.reset();
		
	
		
		tupleLeft = tableScanLeft.nextTuple();
		tupleRight = tableScanRight.nextTuple();
		

	//	numberOfBuckets = Math.max(1, Math.max(tableLeft.getNumberOfBlocks(transaction), tableRight.getNumberOfBlocks(transaction)));
		//TODO 
		numberOfBuckets = 4;
		
		HashMap<String, Bucket> bucketMapLeft = new HashMap<String, Bucket>();
		HashMap<String, Bucket> bucketMapRight = new HashMap<String, Bucket>();
		
		int columnJoinLeft = tableLeft.getIdColumn(attributesOperatorsValues.get(0).getAtribute());
		int columnJoinRight = tableRight.getIdColumn(attributesOperatorsValues.get(0).getValue());
		String operator = attributesOperatorsValues.get(0).getOperator();
		

		
		//particionamento left-side
			while ( tupleLeft != null) { 
				
				String hashID = hashString(tupleLeft.getColunmData(columnJoinLeft));
				
				if(bucketMapLeft.containsKey(hashID)){
					
					Bucket bucket = bucketMapLeft.get(hashID);
					bucket.add(tupleLeft);
					
				}else{
					Bucket bucket = new Bucket(hashID,transaction,tableLeft.getColumns()); 
					bucket.add(tupleLeft);
					bucketMapLeft.put(hashID, bucket); 
					
				}
				
				
				tupleLeft = tableScanLeft.nextTuple();
			}

			
	
			
			//particionamento right side
			while (tupleRight != null) {

				String hashID = hashString(tupleRight.getColunmData(columnJoinRight));
				
				if(bucketMapRight.containsKey(hashID)){
					
					Bucket bucket = bucketMapRight.get(hashID);
					bucket.add(tupleRight);
				
				}else{
					Bucket bucket = new Bucket(hashID,transaction,tableRight.getColumns()); 
					bucket.add(tupleRight);
					bucketMapRight.put(hashID, bucket); 
								
				}

				tupleRight = tableScanRight.nextTuple();
		
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
