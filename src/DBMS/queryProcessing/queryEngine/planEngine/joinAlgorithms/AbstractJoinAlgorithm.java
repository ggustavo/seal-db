package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;



import java.awt.Color;
import java.util.List;

import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.transactionManager.ITransaction;

public abstract class AbstractJoinAlgorithm {
	
	protected IJoinAlgotithmListener ijl;
	protected ITransaction transaction;
	protected ITable tableLeft;
	protected ITable tableRight;
	protected List<Condition> attributesOperatorsValues; 
	
	
	
	public abstract void execute(ITable resultTable);
	
	
	boolean isInsertable = false;
	public boolean match(ITuple tr,ITuple ts){
		isInsertable = false;
		
		if(attributesOperatorsValues.isEmpty())return true;
		
		for (Condition aOv : attributesOperatorsValues) {
			
			if(AbstractPlanOperation.makeComparison(tr.getColunmData(tableLeft.getIdColumn(aOv.getAtribute())), aOv.getOperator(), ts.getColunmData(tableRight.getIdColumn(aOv.getValue())))){
				isInsertable = true;
			}else{
				isInsertable = false;
				break;
			}	
		} 
		return isInsertable;
	}
	
	
	protected void eraseLeft(){
		if(ijl!=null)ijl.resetLeftSide();
	}
	protected void eraseRight(){
		if(ijl!=null)ijl.resetRightSide();
	}
	protected void eraseTotalCenter(){
		if(ijl!=null)ijl.resetTotalCenter();
	}

	protected void showTupleRight(ITuple tuple){
		if(ijl!=null){
			String s = "";
			for (Condition aOv : attributesOperatorsValues) {
				s+=tuple.getColunmData(tableRight.getIdColumn(aOv.getValue()))+" | ";
			}
			if(attributesOperatorsValues.isEmpty()) s = tuple.getColunmData(0);
			ijl.drawTuple(tuple,s, IJoinAlgotithmListener.RIGHT_SIDE, Color.BLACK);			
		}
	}

	protected void showTupleLeft(ITuple tuple) {
		if (ijl != null) {
			String s = "";
			for (Condition aOv : attributesOperatorsValues) {
				s += tuple.getColunmData(tableLeft.getIdColumn(aOv.getAtribute())) + " | ";
			}
			if(attributesOperatorsValues.isEmpty()) s = tuple.getColunmData(0);
			ijl.drawTuple(tuple, s, IJoinAlgotithmListener.LEFT_SIDE, Color.BLACK);
		}

	}
	protected void showMatch(ITuple tuple1,ITuple tuple2,Color c){
		if(ijl!=null)ijl.drawMatchLine(tuple1, tuple2, c);
	}
	protected void showMatch(ITuple tuple1,Bucket bucket,Color c){
		if(ijl!=null)ijl.drawMatchLine(tuple1, bucket, Color.ORANGE);
	}
	
	protected void showMatch(Bucket bucket1,Bucket bucket2,Color c){
		if(ijl!=null)ijl.drawMatchLine(bucket1, bucket2, Color.ORANGE);
	}
	
	protected void showBlockLeft(BlockScan blockScan){
		if(ijl!=null)ijl.drawBlock(blockScan, IJoinAlgotithmListener.LEFT_SIDE);
	}
	
	protected void showBucketLeft(Bucket bucket, int numberOfBuckets,int num,String s){
		if(ijl!=null)ijl.drawBucketLeft(bucket, numberOfBuckets, num, s);
	}
	protected void showBucketRight(Bucket bucket, int numberOfBuckets,int num,String s){
		if(ijl!=null)ijl.drawBucketRight(bucket, numberOfBuckets, num, s);
	}
	
	protected void showBlockRight(BlockScan blockScan){
		if(ijl!=null)ijl.drawBlock(blockScan, IJoinAlgotithmListener.RIGHT_SIDE);
	}
	
	public IJoinAlgotithmListener getiJoinAlgotithmListener() {
		return ijl;
	}

	public void setiJoinAlgotithmListener(IJoinAlgotithmListener iJoinAlgotithmListener) {
		this.ijl = iJoinAlgotithmListener;
	}

	public ITransaction getTransaction() {
		return transaction;
	}

	public void setTransaction(ITransaction transaction) {
		this.transaction = transaction;
	}

	public ITable getTableLeft() {
		return tableLeft;
	}

	public void setTableLeft(ITable tableLeft) {
		this.tableLeft = tableLeft;
	}

	public ITable getTableRight() {
		return tableRight;
	}

	public void setTableRight(ITable tableRight) {
		this.tableRight = tableRight;
	}

	public List<Condition> getAttributesOperatorsValues() {
		return attributesOperatorsValues;
	}

	public void setAttributesOperatorsValues(List<Condition> attributesOperatorsValues) {
		this.attributesOperatorsValues = attributesOperatorsValues;
	}

	
}
