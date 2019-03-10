package graphicalInterface.draw;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.util.HashMap;
import java.util.logging.Level;

import javax.swing.JPanel;

import DBMS.Kernel;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.TupleManipulate;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.Bucket;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.IJoinAlgotithmListener;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import graphicalInterface.util.CliqueY;



public class DrawJoin extends JPanel implements IJoinAlgotithmListener {

	private static final long serialVersionUID = 1L;

	protected BufferedImage img;
	protected JoinOperation join;
	protected int panelSizeY = 0;
	protected int panelSizeX = 770;
	protected Thread aux;
	public int WAIT_TIME = 1000;
	

	private Graphics g;
	
	public static final char LEFT_SIDE = 'l';
	public static final char RIGHT_SIDE = 'r';
	public static final int TUPLE_HEIGHT = 20;
	public static final int TUPLE_WIDTH = 100; 
	public static final int ARR_SIZE = 5;
	
	protected static final int fixedX1 = 10, fixedX2 = 650;
	protected static final int fixedY1 = 90, fixedY2 = 90;
	protected int x1 = fixedX1, x2 = fixedX2;
	protected int y1 = fixedY1, y2 = fixedY2;
	
	private HashMap<ITuple, Point> hashTupleLeft;
	private HashMap<ITuple, Point> hashTupleRight;
	private HashMap<Bucket, Point> hashBucketLeft;
	private HashMap<Bucket, Point> hashBucketRight;
	
	
	public DrawJoin(JoinOperation join) {
		this.hashBucketLeft = new HashMap<Bucket, Point>();
		this.hashBucketRight = new HashMap<Bucket, Point>();
		this.hashTupleLeft = new HashMap<ITuple, Point>();
		this.hashTupleRight = new HashMap<ITuple, Point>();
		this.join = join;
		panelSizeY = 1000;
		this.setPreferredSize(new Dimension(panelSizeX, panelSizeY));
	}
	
	public void initialize(){
		this.setBackground(Color.white);
		//this.setName(join.getName());
		
		//panelSizeY = (Math.max(join.getResultLeft().getNumberOfTuples(), join.getResultRight().getNumberOfTuples()) * TUPLE_HEIGHT) + 200;
		panelSizeY = 5000;
		this.setSize(panelSizeX, panelSizeY);
		this.setBorder(null);
		this.setPreferredSize(new Dimension(panelSizeX, panelSizeY+1));
		this.revalidate();
		this.repaint();
		this.img = new BufferedImage(panelSizeX, panelSizeY, BufferedImage.TYPE_INT_RGB);
		this.setCursor(Cursor.getPredefinedCursor(Cursor.MOVE_CURSOR));
		this.setLayout(null);
		this.setVisible(true);
		CliqueY a = new CliqueY(this);
		this.addMouseListener(a);
		this.addMouseMotionListener(a);
		g = img.getGraphics();
		g.setColor(Color.white);
		g.fillRect(0, 0, panelSizeX, panelSizeY);
	}
	
	
	public void startThread() {
		if (aux == null) {
			aux = new Thread(new Runnable() {
				@Override
				public void run() {
				
					
					
					resetLeftSide();
					for (int i = 0; i < 5; i++) {
						TupleManipulate t1 = new TupleManipulate(i,new String[]{i+""});
						
						resetRightSide();
						//Bucket b = new Bucket("t");
						//drawBucketRight(b, 4, 10, "t");
						drawTuple(t1,i+"L34567890123", LEFT_SIDE,Color.black);
						for (int j = 0; j < 5; j++) {
							TupleManipulate t2 = new TupleManipulate(i,new String[]{j+""});
							drawTuple(t2,j+"R", RIGHT_SIDE,Color.black);
							
							//drawMatchLine(t2,b,Color.red);
						}
						
						
						
					}
					
					aux = null;
				}
			});
			aux.start();
		}

	}

	protected void refreshAndWait() {
		//revalidate();
		repaint();	
		
		synchronized (this) {
			try {
				wait(WAIT_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		 
	}

	public void paint(Graphics g) {
		super.paintComponent(g);
		g.drawImage(img, 0, 0, this);
	}

	public void drawTuple(ITuple tuple,String data, char side,Color c){
		
		if(side == LEFT_SIDE){
			hashTupleLeft.put(tuple, new Point(x1, y1));
			drawTuple(g, data, x1, y1);
			y1+=TUPLE_HEIGHT;
		}else if(side == RIGHT_SIDE){
			hashTupleRight.put(tuple, new Point(x2, y2));
			drawTuple(g, data, x2, y2);
			y2+=TUPLE_HEIGHT;
		}else{
			Kernel.log(this.getClass()," Side= "+side+" informed invalid",Level.SEVERE);
		}
	}
	
	public void drawMatchLine(ITuple tuple1, ITuple tuple2, Color color){
		boolean aux = false;
		Point p1 = hashTupleLeft.get(tuple1);
		if(p1==null){
			p1 = hashTupleRight.get(tuple1);
			aux = true;
		}
		if(p1 ==null){
			Kernel.log(this.getClass()," [1] Tuple: " + tuple1.getData() + " it was not drawn",Level.WARNING);
			return;
		}
		Point p2 = hashTupleLeft.get(tuple2);
		if(p2==null)p2 = hashTupleRight.get(tuple2);
		if(p2 ==null){
			Kernel.log(this.getClass()," [2] Tuple: " + tuple2.getData() + " it was not drawn",Level.WARNING);
			return;
		}
		g.setColor(color);
		if(aux){
			drawLine(g, p1.x, p1.y+TUPLE_HEIGHT/2, p2.x+TUPLE_WIDTH, p2.y+TUPLE_HEIGHT/2);
		}else{
			drawLine(g, p1.x+TUPLE_WIDTH, p1.y+TUPLE_HEIGHT/2, p2.x, p2.y+TUPLE_HEIGHT/2);			
		}
		
		
	}
	
	
	private int yB1 = fixedY1;
	private int yB2 = fixedY2;
	private int bucketLeftInitialX = 230;
	private int bucketRightInitialX = 230;
	
	public void drawBucketLeft(Bucket bucket,int numberOfBuckets,int numberOfTuples,String s){
		g.setColor(Color.blue);
		g.drawRect(fixedX1 + bucketLeftInitialX, yB1, TUPLE_WIDTH, TUPLE_HEIGHT);
		g.drawString(s, fixedX1 + bucketLeftInitialX + 5,yB1 + 15);
		g.setColor(Color.red);
		
		hashBucketLeft.put(bucket, new Point(fixedX1+bucketLeftInitialX-2, yB1));
		if( (int)(numberOfTuples / numberOfBuckets) <= 0){
			yB1 = yB1 + TUPLE_HEIGHT;
		}else{
			
			yB1 = yB1 + (((numberOfTuples / numberOfBuckets)) * TUPLE_HEIGHT);
		}
		refreshAndWait();
	}
	
	public void drawBucketRight(Bucket bucket,int numberOfBuckets,int numberOfTuples,String s){
		g.setColor(Color.blue);
		g.drawRect(fixedX2 - bucketRightInitialX, yB2, TUPLE_WIDTH, TUPLE_HEIGHT);
		g.drawString(s, fixedX2 - bucketRightInitialX + 5, yB2 + 15);
		hashBucketLeft.put(bucket, new Point(fixedX2-bucketRightInitialX+2, yB2));
			
		if( (int)(numberOfTuples / numberOfBuckets) <= 0){
			yB2 = yB2 + TUPLE_HEIGHT;
		}else{
			
			yB2 = yB2 + (((numberOfTuples / numberOfBuckets)) * TUPLE_HEIGHT);
		}
		refreshAndWait();
	}
	
	public void drawMatchLine(ITuple tuple1, Bucket bucket, Color color){
		boolean aux = false;
		Point p1 = hashTupleLeft.get(tuple1);
		if(p1==null){
			p1 = hashTupleRight.get(tuple1);
			aux = true;
		}
		if(p1 ==null){
			Kernel.log(this.getClass()," [3] Tuple: " + tuple1.getData() + " it was not drawn",Level.WARNING);
			return;
		}
		Point p2 = hashBucketLeft.get(bucket);
		if(p2==null)p2 = hashBucketRight.get(bucket);
		if(p2 ==null){
			Kernel.log(this.getClass()," [4] Bucket: " + bucket.getId() + " it was not drawn",Level.WARNING);
			return;
		}
		g.setColor(color);
		if(aux){
			drawArrow(g, p1.x, p1.y+TUPLE_HEIGHT/2, p2.x+TUPLE_WIDTH, p2.y+TUPLE_HEIGHT/2);
		}else{
			drawArrow(g, p1.x+TUPLE_WIDTH, p1.y+TUPLE_HEIGHT/2, p2.x, p2.y+TUPLE_HEIGHT/2);			
		}
		
		
	}
	public void drawMatchLine(Bucket bucket1, Bucket bucket2, Color color){
		boolean aux = false;
		Point p1 =  hashBucketLeft.get(bucket1);
		if(p1==null){
			p1 = hashBucketRight.get(bucket1);
			aux = true;
		}
		if(p1 ==null){
			Kernel.log(this.getClass()," [5] Bucket: " + bucket1.getId() + " it was not drawn",Level.WARNING);
			return;
		}
		Point p2 = hashBucketLeft.get(bucket2);
		if(p2==null)p2 = hashBucketRight.get(bucket2);
		if(p2 ==null){
			Kernel.log(this.getClass()," [6] Bucket: " + bucket2.getId() + " it was not drawn",Level.WARNING);
			return;
		}
		g.setColor(color);
		if(aux){
			drawLine(g, p1.x, p1.y+TUPLE_HEIGHT/2, p2.x+TUPLE_WIDTH, p2.y+TUPLE_HEIGHT/2);
		}else{
			drawLine(g, p1.x+TUPLE_WIDTH, p1.y+TUPLE_HEIGHT/2, p2.x, p2.y+TUPLE_HEIGHT/2);			
		}
		refreshAndWait();
		
	}
	
	
	public void drawArrow(ITuple tuple1, ITuple tuple2, Color color){
		boolean aux = false;
		Point p1 = hashTupleLeft.get(tuple1);
		if(p1==null){
			p1 = hashTupleRight.get(tuple1);
			aux = true;
		}
		if(p1 ==null){
			Kernel.log(this.getClass()," [7] Tuple: " + tuple1.getData() + " it was not drawn",Level.WARNING);
			return;
		}
		Point p2 = hashTupleLeft.get(tuple2);
		if(p2==null)p2 = hashTupleRight.get(tuple2);
		
		if(p2 ==null){
			Kernel.log(this.getClass()," [8] Tuple: " + tuple2.getData() + " it was not drawn",Level.WARNING);
			return;
		}
		g.setColor(color);
		if(aux){
			drawArrow(g, p1.x, p1.y+TUPLE_HEIGHT/2, p2.x+TUPLE_WIDTH, p2.y+TUPLE_HEIGHT/2);
		}else{
			drawArrow(g, p1.x+TUPLE_WIDTH, p1.y+TUPLE_HEIGHT/2, p2.x, p2.y+TUPLE_HEIGHT/2);			
		}
	}
	public void resetLeftSide(){
		hashTupleLeft.clear();
		g.setColor(Color.white);
		g.fillRect(fixedX1-2, fixedY1-2, bucketLeftInitialX-1, Math.max((y1-fixedY1+5 ), yB1+5));
		//resetTotalCenter();
		x1 = fixedX1; //10
		y1 = fixedY1; //90
		//revalidate();
		repaint();
	}
	public void resetRightSide(){
		hashTupleRight.clear();
		g.setColor(Color.WHITE);
		g.fillRect((fixedX2 - bucketRightInitialX )+TUPLE_WIDTH+2, fixedY2-2,fixedX2-2 + TUPLE_WIDTH+5, Math.max(y2-fixedY2+5,yB2+5) );
		
		//g.setColor(Color.CYAN);
		///g.fillRect(fixedX2-2 , fixedY2-2, TUPLE_WIDTH+5, y2-fixedY2+5 );
		//resetTotalCenter();
		x2 = fixedX2; //650
		y2 = fixedY2; //90
		//revalidate();
		repaint();
	}
	
	public void resetTotalCenter(){
		g.setColor(Color.WHITE);
		g.fillRect(fixedX1+TUPLE_WIDTH+1, fixedY1, fixedX2-TUPLE_WIDTH-10, fixedY2 + Math.max(Math.max( y1, y2), (int)getToolkit().getScreenSize().getHeight()));
		g.fillRect(fixedX2-2 , fixedY2-2, TUPLE_WIDTH+5, y2-fixedY2+5 );
	}
	
	
	
	
	private void drawArrow(Graphics g1, int x1, int y1, int x2, int y2) {
        Graphics2D g = (Graphics2D) g1.create();
        double dx = x2 - x1, dy = y2 - y1;
        double angle = Math.atan2(dy, dx);
        int len = (int) Math.sqrt(dx*dx + dy*dy);
        AffineTransform at = AffineTransform.getTranslateInstance(x1, y1);
        at.concatenate(AffineTransform.getRotateInstance(angle));
        g.transform(at);
        g.drawLine(0, 0, len, 0);
        g.fillPolygon(new int[] {len, len-ARR_SIZE, len-ARR_SIZE, len},new int[] {0, -ARR_SIZE, ARR_SIZE, 0}, 4);
        this.refreshAndWait();
    }
	private void drawTuple(Graphics g, String tupleData,int x, int y) {
		g.setColor(Color.black);
		g.drawRect(x, y, TUPLE_WIDTH, TUPLE_HEIGHT);
		g.drawString(formatString(tupleData), x + 5, y + 15);
		this.refreshAndWait();
	}
	private void drawLine(Graphics g, int x1, int y1, int x2, int y2){
		g.drawLine(x1, y1, x2, y2);
	}
	
	public void drawBlock(BlockScan blockScan, char side) {
		if(side == LEFT_SIDE){			
			//LogError.save(this.getClass(),"Block L: " + blockScan.getAtualBlock());
			g.setColor(Color.WHITE);
			g.fillRect(fixedX1-10, fixedY1-30, TUPLE_WIDTH+20, fixedY1-7);
			g.setColor(Color.BLUE);
			g.drawString("Block: " + blockScan.getAtualBlock(), fixedX1+30, fixedY1-7);
			//g.drawRect(x1-3, y1-22, 100+6, (20*t1.size())+25);
			
		}else if(side == RIGHT_SIDE){
			//LogError.save(this.getClass(),"Block R: " + blockScan.getAtualBlock());
			g.setColor(Color.WHITE);
			g.fillRect(fixedX2-10, fixedY2-30, TUPLE_WIDTH+20, fixedY2);
			g.setColor(Color.BLUE);
			g.drawString("Block: " + blockScan.getAtualBlock(), fixedX2+30, fixedY2-7);
			//g.drawRect(x2-3, y2-22, 100+6, (20*t2.size())+25);
			
			}
		
		
		
	}
	
	private String formatString(String s){		
		if(s.length()<=(TUPLE_WIDTH/10)+2)return s;
		StringBuffer s2 = new StringBuffer();
		for (int i = 0; i < (TUPLE_WIDTH/10)+2; i++) {
			s2.append(s.charAt(i));
		}
		s2.append("...");
		return s2.toString();
	}
	
	public int getWAIT_TIME() {
		return WAIT_TIME;
	}

	public void setWAIT_TIME(int wAIT_TIME) {
		WAIT_TIME = wAIT_TIME;
	}
	/*
	public static void main(String[] args) {	
		JFrame j = new JFrame();
		j.setSize(10000, 1000);
		j.setLayout(null);
		
		DrawJoin dj = new DrawJoin(null);
		dj.initialize();
		dj.startThread();
		
		j.add(dj);
		j.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		j.setVisible(true);
	}
	*/


}
