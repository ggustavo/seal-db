package graphicalInterface.draw;


import java.awt.Color;
import java.awt.Graphics;
import java.awt.GridLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.model.mxCell;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.util.mxConstants;
import com.mxgraph.util.mxUtils;
import com.mxgraph.view.mxGraph;
import com.mxgraph.view.mxStylesheet;

import DBMS.Kernel;
import DBMS.connectionManager.DispatcherListener;
import DBMS.distributed.resourceManager.message.MessageHeader;
import graphicalInterface.images.ImagensController;

public class DrawNodesGraph extends JPanel implements DispatcherListener{

	private static final long serialVersionUID = 1L;
	private mxGraph graph;
	public CustomMxGraphComponent graphComponent;
	private HashMap<String, Object> vertexMap;
	private List<Edge> edges;

	
	class Edge{
		String v1;
		String v2;
	}

//	private final String VERTEX_STYLE = mxConstants.STYLE_SHAPE + "=" + mxConstants.SHAPE_RECTANGLE + ";"
//			+ JGraphStyle.STROKECOLOR.mxStyle + ";" + JGraphStyle.FONTCOLOR.mxStyle + ";"
//			+ JGraphStyle.FONTSTYLE.mxStyle + ";" + JGraphStyle.FILLCOLOR.mxStyle+";"+mxConstants.STYLE_STROKEWIDTH+"=2";
	
	private final String VERTEX_STYLE = mxConstants.STYLE_SHAPE + "=" + mxConstants.SHAPE_IMAGE + ";"+
			mxConstants.STYLE_IMAGE + "=" + ImagensController.class.getResource("database_large.png") + ";"+
			mxConstants.STYLE_VERTICAL_LABEL_POSITION+"="+mxConstants.ALIGN_BOTTOM+";"
			+ JGraphStyle.STROKECOLOR.mxStyle + ";" + JGraphStyle.FONTCOLOR.mxStyle+";"
			+ JGraphStyle.FONTSTYLE.mxStyle + ";" + JGraphStyle.FILLCOLOR.mxStyle+";"+mxConstants.STYLE_STROKEWIDTH+"=2";
	
	//ImagensController.class.getResource("database.png")
	class CustomMxGraphComponent extends mxGraphComponent{
		private static final long serialVersionUID = 1L;
		

		
		public CustomMxGraphComponent(mxGraph arg0) {
			super(arg0);
			points = new LinkedList<>();
		}
		
		public class Msg{
			int x; 
			int y;
			Color c;
		}
		
		List<Msg> points;
		
		@Override
		public void paint(Graphics g) {
			super.paint(g);
			
			for (Msg p : points) {
				g.setColor(p.c);
				g.fillRect(p.x+2, p.y+5, 28, 20);
				g.drawImage(ImagensController.MSG.getImage(), p.x, p.y, 30,30, null);				
			}
			
		}
		/*
		private static final float MIN_BRIGHTNESS = 0.8f;
		private Random random = new Random();
		
		 private Color createRandomBrightColor() {
		        float h = random.nextFloat();
		        float s = random.nextFloat();
		        float b = MIN_BRIGHTNESS + ((1f - MIN_BRIGHTNESS) * random.nextFloat());
		        Color c = Color.getHSBColor(h, s, b);
		        return c;
		    }
		*/
		public void drawAnimation(mxCell v1, mxCell v2, Color c) {
//			
//			new Thread(new Runnable() {
//				
//				
//				@Override
//				public void run() {
//					
//				}
//			}).start();
		
			int n = 15;
			Msg p = new Msg();
			p.c = c;
			points.add(p);
			for (int i = 0; i < n; i++) {
				
				if(v1!=null&&v2!=null) {
					subPoint(p,
							v1.getGeometry().getCenterX(),
							v1.getGeometry().getCenterY(), 
							v2.getGeometry().getCenterX(),
							v2.getGeometry().getCenterY(), 
							i, n);
					repaint();							
				}
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}		
			points.remove(p);
			
		}
		
		
		public void subPoint(Msg p1, double startPointX, double startPointY, double endPointX, double endPointY, int segment, int totalSegments) {
			  double midX = (startPointX + (int) ((double) (endPointX - startPointX) / (double) totalSegments) * segment);
			  double midY = (startPointY + (int) ((double) (endPointY - startPointY) / (double) totalSegments) * segment);
			  p1.x = (int) midX;
			  p1.y = (int) midY;   
			}
		
	}
	
	
	
	public DrawNodesGraph() {
		setLayout(new GridLayout(1, 1));
		drawGraph();
		applyEdgeDefaults();
		vertexMap = new HashMap<>();
		edges = new LinkedList<>();
		this.add(graphComponent);

	}

	
	public void showMsg(String v1, String v2, char type) {
		
		Object vO1 = vertexMap.get(v1);
		Object vO2 = vertexMap.get(v2);
		
		if (vO1 == null && vO2 == null) {
			drawVertex(v1);
			drawVertex(v2);	
			drawEdge(v1, v2);
			vO1 = vertexMap.get(v1);
			vO2 = vertexMap.get(v2);	
		}else if(vO1 == null) {
			drawVertex(v2);	
			drawEdge(v1, v2);
			vO1 = vertexMap.get(v1);
			vO2 = vertexMap.get(v2);
		}else if(vO2 == null) {
			drawVertex(v2);	
			drawEdge(v1, v2);
			vO1 = vertexMap.get(v1);
			vO2 = vertexMap.get(v2);
		}
		
		Color color = Color.WHITE;
		
		switch (type) {
		
		case MessageHeader.NEW_CONNECTION:
			color = new Color(102, 255, 153); //verde
		break;
	
		case MessageHeader.CLOSE_CONNECTION:
			color = new Color(204, 102, 255); //roxo
		break;
		
		case MessageHeader.BEGIN_TRANSACTION:
			color = new Color(255, 255, 153); //Amarelo
		break;
		
		case MessageHeader.COMMIT_TRANSACTION:
			color = new Color(51, 204, 51);
		break;
		
		case MessageHeader.PREPARE:
			color = new Color(102, 204, 255);
		break;
		
		case MessageHeader.ABORT:
			color = new Color(255, 0, 0);
		break;
		
		case MessageHeader.STATUS:
			color = new Color(204, 51, 255);
		break;
		
		case MessageHeader.EXECUTE_QUERY_SQL:
			color = new Color(102, 102, 153);
		break;
		case MessageHeader.LOG_TRANSACTION:
			color = new Color(102, 102, 153);
		break;
		case MessageHeader.RESPONSE:
			color = new Color(128, 229, 255);
		break;
		
		case MessageHeader.MULTI_RESPONSE:
			color = new Color(255, 153, 102);
		break;
		
		default:
		break;
	}
		
		
		
		
		
		for (Edge edge : edges) {
			if((edge.v1.equals(v1) && edge.v2.equals(v2)) || (edge.v1.equals(v2) && edge.v2.equals(v1)) ) {
				graphComponent.drawAnimation((mxCell)vO1, (mxCell)vO2, color); //graphComponent.createRandomBrightColor()
				return;
			}	
		}
		
		
	}
	

	private void layoutGraph() {
		mxCircleLayout layout = new mxCircleLayout(graph);
		layout.setRadius(200);
		layout.setX0(50);
		layout.setY0(50);
		//layout.interRankCellSpacing=40;
		Object cell = graph.getDefaultParent();
		graph.getModel().beginUpdate();
		try {
			layout.execute(cell);
		} finally {
			graph.getModel().endUpdate();
		}
	}

	public void drawGraph() {
		graph = new mxGraph();
		graph.setCellsEditable(false);
		graph.setConnectableEdges(false);
		graph.setDisconnectOnMove(false);
		graphComponent = new CustomMxGraphComponent(graph);
		graphComponent.setConnectable(false);
		//graphComponent.setPreferredSize(new Dimension((int) dimension.getWidth() / 2, (int) dimension.getHeight()));
		graphComponent.getViewport().setOpaque(true);
		graphComponent.getViewport().setBackground(Color.WHITE);	
	}
	
	
	
	public void drawVertex(final String name) {
		
		
		SwingUtilities.invokeLater(new Runnable() {

			@Override
			public void run() {
				if (!vertexMap.containsKey(name)) {
					graph.getModel().beginUpdate();
					Object parent = graph.getDefaultParent();
					Object v = graph.insertVertex(parent, null, name, 0, 0,64, 64, VERTEX_STYLE);
					graph.getModel().endUpdate();
					vertexMap.put(name, v);
					layoutGraph();
				}else{
					updateColorVertex(name, "black");
				}
			}
		});
		
		
	}
	
	
	public mxCell getVertex(String name) {
		return (mxCell)vertexMap.get(name);
	}
	

	public void drawEdge(final String v1, final String v2) {
		
		SwingUtilities.invokeLater(new Runnable() {

			@Override
			public void run() {
				Object vO1 = vertexMap.get(v1);
				Object vO2 = vertexMap.get(v2);
				if (vO1 != null && vO2 != null) {
					Object parent = graph.getDefaultParent();
					graph.getModel().beginUpdate();
					graph.insertEdge(parent, null, "", vO1, vO2);
					Edge e = new Edge();
					e.v1 = v1;
					e.v2 = v2;
					edges.add(e);
					//mxCell v = (mxCell)vO1;
					//System.out.println(v.getGeometry().getPoint());
					
					graph.getModel().endUpdate();
				}
			}
		});
	}
	


	public void removeVertex(final String vertex) {
		
		SwingUtilities.invokeLater(new Runnable() {

			@Override
			public void run() {
				Object v = vertexMap.remove(vertex);
				if (v != null) {
					graph.removeCells(new Object[] { v });
					
				}
			}
		});
		
	
	}
	
	public void removeEdges(final String v1, final String v2){
		SwingUtilities.invokeLater(new Runnable() {
			
			@Override
			public void run() {
				graph.getModel().beginUpdate();
                try {
                	Object vO1 = vertexMap.get(v1);
    				Object vO2 = vertexMap.get(v2);
    				
    				if (vO1 != null && vO2 != null) {
    					Object[] edges = graph.getEdgesBetween(vO1, vO2);
    					
    					for( Object edge: edges) {
    						graph.getModel().remove( edge);
    					}	
    				}
                } finally {
                    graph.getModel().endUpdate();
                }				
			}
		});
	}
	
	public void updateColorVertex(final String vertex,final String color){
		SwingUtilities.invokeLater(new Runnable() {

			@Override
			public void run() {
				Object v = vertexMap.get(vertex);
				if(v!=null){
					graph.setCellStyles(mxConstants.STYLE_FONTCOLOR, color, new Object[]{v});
					graphComponent.refresh();			
				}
			}
		});
		
		
	}

	private void applyEdgeDefaults() {

		Map<String, Object> edge = new HashMap<String, Object>();

		edge.put(mxConstants.STYLE_ROUNDED, false);
		edge.put(mxConstants.STYLE_STROKECOLOR, "#000000"); // default is
															// #6482B9
		edge.put(mxConstants.STYLE_STROKEWIDTH, "1.5");
		edge.put(mxConstants.STYLE_FONTCOLOR, "#000000");
		edge.put(mxConstants.STYLE_FONTSIZE, "24");
		edge.put(mxConstants.STYLE_SHAPE, mxConstants.SHAPE_CONNECTOR);
	
		
		
		//edge.put(mxConstants.STYLE_EDGE, mxConstants.EDGESTYLE_SIDETOSIDE);
		edge.put(mxConstants.STYLE_ORTHOGONAL, false);
		//edge.put(mxConstants.STYLE_ENDARROW, mxConstants.ARROW_CLASSIC);
		edge.put(mxConstants.STYLE_VERTICAL_ALIGN, mxConstants.ALIGN_MIDDLE);
		edge.put(mxConstants.STYLE_ALIGN, mxConstants.ALIGN_CENTER);
		
		mxStylesheet edgeStyle = new mxStylesheet();
		edgeStyle.setDefaultEdgeStyle(edge);
		graph.setStylesheet(edgeStyle);
		

	}

	public enum JGraphStyle {

		FILLCOLOR(mxConstants.STYLE_FILLCOLOR, mxUtils.getHexColorString(Color.WHITE)), 
		STROKECOLOR(mxConstants.STYLE_STROKECOLOR, mxUtils.getHexColorString(Color.BLACK)), 
		FONTSTYLE(mxConstants.STYLE_FONTSTYLE, mxConstants.FONT_BOLD), 
		FONTCOLOR(mxConstants.STYLE_FONTCOLOR,mxUtils.getHexColorString(Color.BLACK));
		
		public String mxStyle;

		JGraphStyle(Object... values) {
			
			mxStyle = "";
			for (int i = 0; i < values.length; i++) {
				if (i % 2 == 0) {
					mxStyle += values[i] + "=";
				} else {
					mxStyle += values[i] + ";";
				}
			}
		}
	}
	public static void main(String[] args) throws InterruptedException {
		
		
		open();
		

	}

	 public static JFrame open(){
		 
		 	final DrawDistibutedTests k = new DrawDistibutedTests();
		 	
		 	k.setTitle("Node Connections " + Kernel.getTransactionManager().getDispatcher().getName());
			k.setSize(1200, 700);
			k.setIconImage(ImagensController.DISTIBUTED);
			k.hideNode();
			
			DrawNodesGraph f = new DrawNodesGraph();
			Kernel.getTransactionManager().getDispatcher().setListener(f);
			k.graph.add(f);
			
			
			f.graphComponent.getGraphControl().addMouseListener(new MouseAdapter() {
				 public void mouseClicked(MouseEvent e) {
					 mxCell cell = (mxCell) f.graphComponent.getCellAt(e.getX(), e.getY());
					 if(cell!=null) {
						 k.showNode(cell.getValue().toString());
					 }else {
						 k.hideNode();
					 }
				 }
			});
			
			
			
			k.setVisible(true);
//			f.drawVertex("1");
//			f.drawVertex("2");
//			f.drawVertex("3");
//			f.drawVertex("4");
			
			/*
		for (int i = 5; i < 5; i++) {
			f.drawVertex(i+"");
		}
			
			f.drawEdge("1", "2");
			f.drawEdge("2", "3");
			f.drawEdge("3", "1");
			
			f.drawEdge("1", "4");
			f.drawEdge("2", "4");
			f.drawEdge("3", "4");
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			f.graphComponent.drawAnimation(f.getVertex("1"), f.getVertex("2"),f.graphComponent.createRandomBrightColor());
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			f.graphComponent.drawAnimation(f.getVertex("2"), f.getVertex("3"),f.graphComponent.createRandomBrightColor());
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			f.graphComponent.drawAnimation(f.getVertex("4"), f.getVertex("1"),f.graphComponent.createRandomBrightColor());
			*/
			return k;
	 }

}
