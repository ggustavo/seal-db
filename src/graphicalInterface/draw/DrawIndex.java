package graphicalInterface.draw;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.MouseWheelEvent;
import java.awt.event.MouseWheelListener;
import java.awt.geom.AffineTransform;
import java.awt.geom.Line2D;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JSplitPane;

import DBMS.fileManager.BPlusTree;
import DBMS.fileManager.BPlusTree.InternalNode;
import DBMS.fileManager.BPlusTree.LeafNode;
import DBMS.fileManager.BPlusTree.Node;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;
import graphicalInterface.images.ImagensController;
import graphicalInterface.util.CliqueXY;

public class DrawIndex extends JPanel implements MouseWheelListener{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	 public static JFrame open(){
   	  JFrame jFrame = new JFrame("Index View");
   	  jFrame.setMinimumSize(new Dimension(900, 900));
   	  jFrame.getContentPane().setBackground(Color.white);
   	  
   	  DrawIndex index = new DrawIndex();
   	  index.addMouseWheelListener(index);
   	  CliqueXY m  = new CliqueXY(index);
   	  index.addMouseListener(m);
   	  index.addMouseMotionListener(m);
   	  index.setBounds(0, 0, 2000, 2000);  

   	  JPanel indexExternal = new JPanel();
   	  indexExternal.setLayout(null);
   	  indexExternal.setBackground(Color.white);
  	  indexExternal.add(index);
   	  
   	  
  	  JPanel menu = new DrawIndexMenu(index);
  	  
  	  JSplitPane split = new JSplitPane();
  	  split.setLeftComponent(menu);
  	  split.setRightComponent(indexExternal);
  	  split.setResizeWeight(0.02);
	
  	 
      jFrame.add(split);
      jFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
      jFrame.setIconImage(ImagensController.FRAME_ICON_INDEX_VIEW);
      jFrame.setExtendedState(JFrame.MAXIMIZED_BOTH);
      jFrame.setVisible(true);
   
      return jFrame;
   }
   
	 @Override
	 public void mouseWheelMoved(MouseWheelEvent e) {
	     zoomer = true;
	     //Zoom in
	     if (e.getWheelRotation() < 0) {
	         zoomFactor *= 1.1;
	         repaint();
	     }
	     //Zoom out
	     if (e.getWheelRotation() > 0) {
	         zoomFactor /= 1.1;
	         repaint();
	     }
	 }

	
    @Override
    public void paintComponent(Graphics g) {
    	
    	Graphics2D g2d = (Graphics2D) g;
	
    	 if (zoomer) { //https://stackoverflow.com/questions/6543453/zooming-in-and-zooming-out-within-a-panel
    	        AffineTransform at = new AffineTransform();
    	        at.scale(zoomFactor, zoomFactor);
    	        prevZoomFactor = zoomFactor;
    	        g2d.transform(at);
    	        //zoomer = false;
    	    }
    	
    	
    	//g.setColor(Color.BLACK);
    	//g.drawRect(0, 0, this.getWidth(), this.getHeight());
    	g.setColor(Color.WHITE);
    	setSize(Math.max(this.getWidth(),max_w+20), Math.max(this.getHeight(), max_h+40));
    	g.fillRect(0, 0, Math.max(this.getWidth(),max_w), Math.max(this.getHeight(), max_h));
 	
    	g.setColor(Color.black);
    	g2d.setRenderingHint(
    	    RenderingHints.KEY_ANTIALIASING,
    	    RenderingHints.VALUE_ANTIALIAS_ON);
    	g2d.setRenderingHint(
    	    RenderingHints.KEY_TEXT_ANTIALIASING,
    	    RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
    	
		if (root != null) {
			max_h = max_w = 0;
			drawNode(20, 20, g, root);
			max_w = (root.x * 2) + w;
			g.setColor(Color.lightGray);
			g.drawRect(0, 0, max_w, max_h+(h*2));
       }
    }
    
    @SuppressWarnings("rawtypes")
	private TreeNode<Node> root = null;
    
    private int order = 4;
    private int numberOfKeys = order-1;
    private int w = 50*order;
    private int h = 50;
    
    private int distance_h = 130;
    
    private int string_w = 25;
    private int string_h = 10;
    
    private int arrow_h = 14;
    private int arrow_w = 7;
    
    private int max_w = 2000;
    private int max_h = 2000;
   
   
    public static double zoomFactor = 1;
    public static double prevZoomFactor = 1;
    private boolean zoomer;
    

//    public void repaint() {
//    	int x = this.getX();
//    	int y = this.getY();
//    	setLocation(x, y);
//    	super.repaint();
//    	setLocation(x, y);
//    }
    
    @SuppressWarnings("rawtypes")
	public int drawNode(int x, int y, Graphics g, TreeNode<Node> node){
    	g.setColor(Color.BLACK);
		if (node.children.isEmpty()) {
			
			BPlusTree.LeafNode data = (LeafNode) node.data;
			
			for (int i = 0; i < numberOfKeys; i++) {
				g.setColor(Color.BLACK);
				g.drawRect(x, y, w/(numberOfKeys+1) + i * w/(numberOfKeys+1), h);
				String s = "null";
				g.setColor(Color.RED);
				if(i < data.values.size() && data.values.get(i)!=null) {
					g.setColor(Color.BLACK);
					s = data.values.get(i).toString();
				}
				g.drawString(s, x + (w/(numberOfKeys+1) + (i*(w/(numberOfKeys+1)))) / 2 - string_w/2 + (i*w/(numberOfKeys+1))/2, y + h / 2 + string_h / 2);	
				g.setColor(Color.BLACK);
				drawArrowLine(g, x + (w/(numberOfKeys+1) + (i*(w/(numberOfKeys+1)))) / 2 + (i*w/(numberOfKeys+1))/2, y + h, x + (w/(numberOfKeys+1) + (i*(w/(numberOfKeys+1)))) / 2 + (i*w/(numberOfKeys+1))/2, y + (h*2), arrow_h, arrow_w);
			}
			drawArrowLine(g, x + w - (w/(numberOfKeys+1)), y + h -(h/2), x + w-5, y + h -(h/2), arrow_h, arrow_w);
		//	g.drawRect(x, y, w/(order+1) +  order* w/(order+1), h);
			
			node.x = x;
			node.y = y;
			if(y > max_h)max_h = y+ h +1;
			return w;
		} else {
			int childrens_size = 0;

			for (TreeNode<Node> n : node.children) {
				childrens_size += drawNode(x + childrens_size, y + distance_h, g, n);
			}

			node.x = x + childrens_size / 2 - w / 2;
			node.y = y;

			BPlusTree.InternalNode data = (InternalNode) node.data;
			for (int i = 0; i < numberOfKeys; i++) {
				g.drawRect(node.x, y, w/numberOfKeys + i * w/numberOfKeys, h);
				String s = "null";
				g.setColor(Color.RED);
				if(i < data.keys.size() && data.keys.get(i)!=null) {
					g.setColor(Color.BLACK);
					s = data.keys.get(i).toString();
				}
				g.drawString(s, node.x + (w/numberOfKeys + (i*(w/numberOfKeys))) / 2 - string_w/2 + (i*w/numberOfKeys)/2, y + h / 2 + string_h / 2);
				g.setColor(Color.BLACK);
			}
			g.setColor(Color.BLACK);
			
			int dh = 0;
			for (TreeNode<Node> n : node.children) {
		
//				if(node.children.size() > 1){
//					drawArrowLine(g, node.x+dh, node.y+h + 5, n.x + w - (w/2), n.y, arrow_h, arrow_w);
//					dh += (w/(node.children.size()-1));
//				}else{
//					drawArrowLine(g, node.x+w -(w/2), node.y+h + 5, n.x + w - (w/2), n.y, arrow_h, arrow_w);
//				}
				if(n.children.isEmpty()) {
					drawArrowLine(g, node.x+dh , node.y+h, n.x + (w-(w/order)) - ((w-(w/order))/2), n.y, arrow_h, arrow_w);	
				}else {
					drawArrowLine(g, node.x+dh , node.y+h, n.x + w - (w/2), n.y, arrow_h, arrow_w);	
					
				}
				
				dh+=w/(numberOfKeys);
			}

			return childrens_size;
		}
	}

    

	
	
	public String projectValue(Object o) {
		if(o==null) {
			String s = "null";
			for (int j = 0; j < (17-4)/2; j++) {
				s = " "+s+" ";
			}
			return s;
		}else {
			String s = "";
			int i = 0;
			for (i = 0; i < Math.min(17, o.toString().length()); i++) {
				s+=o.toString().charAt(i)+"";
			}
			
			s+="...";
			
			if(i+3<17) {
				for (int j = 0; j < i/2; j++) {
					s = " "+s+" ";
				}
				
			}
			
			return s;
		}
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" }) 
	public void drawTree(Node n, TreeNode<Node> t) {
		if(n instanceof InternalNode) {
			List<Node> list = ((InternalNode) n).children;
			
			for (Node child : list) {
				drawTree(child, t.addChild(child));
			}
		}
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public DrawIndex load(ITransaction transaction, ITable table, Column column, int o){
		
		max_w = 2000;
		max_h = 2000;
		
		root = null;
		order = o;
		numberOfKeys = order-1;
		w = 50*order;
		h = 50;
		
//		BPlusTree  bpt2 = new BPlusTree<Double, String>(order);	
//		
//		bpt2.insert(1, "1");
//		bpt2.insert(2, "2");
//		bpt2.insert(3, "3");
//		bpt2.insert(4, "4");
//		bpt2.insert(5, "5");
//		bpt2.insert(6, "6");
//		
//		System.out.println(bpt2.toString());
//		
//		bpt2.insert(2, "211");
//		bpt2.insert(2, "222");
//		bpt2.insert(2, "233");
//		
//		System.out.println(bpt2.toString());
//		
//		root = new TreeNode<BPlusTree.Node>(bpt2.getRoot());
//		drawTree(bpt2.getRoot(), root);
//		
//		revalidate();
//		repaint();
//		
//		if(true)return this;
		
		if(table==null) {
			revalidate();
			repaint();
			return this;
		}
		setSize(this.getWidth(), this.getHeight());

		BPlusTree bpt = null;
	
		TableScan tableScan = new TableScan(transaction, table);
	
		ITuple tuple = tableScan.nextTuple();
		
		if(tuple != null) {
			String value = tuple.getColunmData(table.getIdColumn(column.getName())).trim();
			boolean isNumeric = true;
			try {
				Double.parseDouble(value);
			}catch (Exception e) {
				isNumeric = false;
			}
			
			if(isNumeric) {
				bpt = new BPlusTree<Double, String>(order);	
			}else {
				bpt = new BPlusTree<String, String>(order);
			}
		}
		
		
		while (tuple != null) {
			
			
			String value = tuple.getColunmData(table.getIdColumn(column.getName())).trim();
			Double valueNumeric = 0.0;
			boolean isNumeric = true;
			try {
				valueNumeric = Double.parseDouble(value);
			}catch (Exception e) {
				isNumeric = false;
			}
			
			if(isNumeric) {
				bpt.insert(valueNumeric,valueNumeric+"");
			}else {
				value = projectValue(value);
				bpt.insert(value,value);
				w = 100*order;
				string_w = 85;
			}
			
			
			tuple = tableScan.nextTuple();
		}
	
		//System.out.println(bpt.toString());

		root = new TreeNode<BPlusTree.Node>(bpt.getRoot());
		drawTree(bpt.getRoot(), root);
		
		revalidate();
		repaint();
		
		return this;
	}
    
    private void drawArrowLine(Graphics g, int x1, int y1, int x2, int y2, int d, int h) {
     
    	int dx = x2 - x1, dy = y2 - y1;
        double D = Math.sqrt(dx*dx + dy*dy);
        double xm = D - d, xn = xm, ym = h, yn = -h, x;
        double sin = dy / D, cos = dx / D;

        x = xm*cos - ym*sin + x1;
        ym = xm*sin + ym*cos + y1;
        xm = x;

        x = xn*cos - yn*sin + x1;
        yn = xn*sin + yn*cos + y1;
        xn = x;

        int[] xpoints = {x2, (int) xm, (int) xn};
        int[] ypoints = {y2, (int) ym, (int) yn};
        
        Graphics2D g2 = (Graphics2D) g;
        
        g2.setStroke(new BasicStroke(1));
        g2.draw(new Line2D.Float(x1, y1, x2, y2));
        g2.setStroke(new BasicStroke(1));
        
      //  g.drawLine(x1, y1, x2, y2);
        
        g.fillPolygon(xpoints, ypoints, 3);
    }
   
	static class TreeNode<T> {

		public T data;
		TreeNode<T> parent;
		List<TreeNode<T>> children;
		int x;
		int y;

		public TreeNode(T data) {
			this.data = data;
			this.children = new LinkedList<TreeNode<T>>();
		}

		public TreeNode<T> addChild(T child) {
			TreeNode<T> childNode = new TreeNode<T>(child);
			childNode.parent = this;
			this.children.add(childNode);
			return childNode;
		}

	}
    
}