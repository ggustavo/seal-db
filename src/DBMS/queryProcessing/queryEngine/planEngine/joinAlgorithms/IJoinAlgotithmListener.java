package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;
import java.awt.Color;

import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;



public interface IJoinAlgotithmListener {

	
	public static final char LEFT_SIDE = 'l';
	public static final char RIGHT_SIDE = 'r';
	
	public void drawTuple(ITuple tuple,String data, char side,Color c);
	
	public void drawMatchLine(ITuple tuple1, ITuple tuple2, Color color);
	
	public void drawArrow(ITuple tuple1, ITuple tuple2, Color color);
	
	public void resetLeftSide();
	
	public void resetRightSide();
	
	public void resetTotalCenter();
	
	public void initialize();

    public void drawBlock(BlockScan blockScan ,char side); 
    
    public void drawMatchLine(ITuple tuple1, Bucket bucket, Color color);
    
    public void drawBucketLeft(Bucket bucket,int numberOfBuckets,int numberOfTuples,String s);
    
    public void drawBucketRight(Bucket bucket,int numberOfBuckets,int numberOfTuples,String s);
    
    
    public void drawMatchLine(Bucket bucket1, Bucket bucket2, Color color);
}
