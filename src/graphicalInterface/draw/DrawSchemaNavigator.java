package graphicalInterface.draw;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.geom.Point2D;
import java.util.concurrent.CountDownLatch;

import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;
import javax.swing.border.LineBorder;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.Column;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import prefuse.Constants;
import prefuse.Display;
import prefuse.Visualization;
import prefuse.action.Action;
import prefuse.action.ActionList;
import prefuse.action.ItemAction;
import prefuse.action.RepaintAction;
import prefuse.action.animate.ColorAnimator;
import prefuse.action.animate.LocationAnimator;
import prefuse.action.animate.QualityControlAnimator;
import prefuse.action.animate.VisibilityAnimator;
import prefuse.action.assignment.ColorAction;
import prefuse.action.assignment.FontAction;
import prefuse.action.filter.FisheyeTreeFilter;
import prefuse.action.layout.CollapsedSubtreeLayout;
import prefuse.action.layout.graph.NodeLinkTreeLayout;
import prefuse.activity.SlowInSlowOutPacer;
import prefuse.controls.ControlAdapter;
import prefuse.controls.FocusControl;
import prefuse.controls.PanControl;
import prefuse.controls.WheelZoomControl;
import prefuse.controls.ZoomControl;
import prefuse.controls.ZoomToFitControl;
import prefuse.data.Node;
import prefuse.data.Schema;
import prefuse.data.Tree;
import prefuse.data.Tuple;
import prefuse.data.event.TupleSetListener;
import prefuse.data.search.PrefixSearchTupleSet;
import prefuse.data.tuple.TupleSet;
import prefuse.render.AbstractShapeRenderer;
import prefuse.render.DefaultRendererFactory;
import prefuse.render.EdgeRenderer;
import prefuse.render.LabelRenderer;
import prefuse.util.ColorLib;
import prefuse.util.FontLib;
import prefuse.util.ui.JFastLabel;
import prefuse.visual.VisualItem;
import prefuse.visual.expression.InGroupPredicate;
import prefuse.visual.sort.TreeDepthItemSorter;

public class DrawSchemaNavigator extends Display{

	private static final long serialVersionUID = 1L;
	private static final String tree = "tree";
    private static final String treeNodes = "tree.nodes";
    private static final String treeEdges = "tree.edges";
    
    private LabelRenderer m_nodeRenderer;
    private EdgeRenderer m_edgeRenderer;
    
    private int m_orientation = Constants.ORIENT_LEFT_RIGHT;
    
    /** Label data field included in generated Graphs */ 
    public static final String LABEL = "label"; 
    
    public static final String LABEL2 = "label2"; 
    
    
    /** Node table schema used for generated Graphs */ 
    public static final Schema LABEL_SCHEMA = new Schema(); 
    static { 
        LABEL_SCHEMA.addColumn(LABEL, String.class, ""); 
        LABEL_SCHEMA.addColumn(LABEL2, String.class, ""); 
    } 
    
    
    public DrawSchemaNavigator(Tree t) {
        super(new Visualization());
      
        m_vis.add(tree, t);
        
        m_nodeRenderer = new LabelRenderer(LABEL);
        m_nodeRenderer.setRenderType(AbstractShapeRenderer.RENDER_TYPE_FILL);
        m_nodeRenderer.setHorizontalAlignment(Constants.LEFT);
        m_nodeRenderer.setRoundedCorner(8,8);
        m_edgeRenderer = new EdgeRenderer(Constants.EDGE_TYPE_LINE);
        
        DefaultRendererFactory rf = new DefaultRendererFactory(m_nodeRenderer);
        rf.add(new InGroupPredicate(treeEdges), m_edgeRenderer);
        m_vis.setRendererFactory(rf);
               
        // colors
        ItemAction nodeColor = new NodeColorAction(treeNodes);
        ItemAction textColor = new ColorAction(treeNodes,
                VisualItem.TEXTCOLOR, ColorLib.rgb(0,0,0));
        m_vis.putAction("textColor", textColor);
        
        ItemAction edgeColor = new ColorAction(treeEdges,
                VisualItem.STROKECOLOR, ColorLib.rgb(200,200,200));
        
        // quick repaint
        ActionList repaint = new ActionList();
        repaint.add(nodeColor);
        repaint.add(new RepaintAction());
        m_vis.putAction("repaint", repaint);
        
        // full paint
        ActionList fullPaint = new ActionList();
        fullPaint.add(nodeColor);
        m_vis.putAction("fullPaint", fullPaint);
        
        // animate paint change
        ActionList animatePaint = new ActionList(400);
        animatePaint.add(new ColorAnimator(treeNodes));
        animatePaint.add(new RepaintAction());
        m_vis.putAction("animatePaint", animatePaint);
        
        // create the tree layout action
        NodeLinkTreeLayout treeLayout = new NodeLinkTreeLayout(tree,m_orientation, 50, 0, 40);
        treeLayout.setLayoutAnchor(new Point2D.Double(25,300));
        m_vis.putAction("treeLayout", treeLayout);
        
        CollapsedSubtreeLayout subLayout = 
            new CollapsedSubtreeLayout(tree, m_orientation);
        m_vis.putAction("subLayout", subLayout);
        
        AutoPanAction autoPan = new AutoPanAction();
        
        // create the filtering and layout
        ActionList filter = new ActionList();
        filter.add(new FisheyeTreeFilter(tree, 2));
        filter.add(new FontAction(treeNodes, FontLib.getFont("Tahoma", 16)));
        filter.add(treeLayout);
        filter.add(subLayout);
        filter.add(textColor);
        filter.add(nodeColor);
        filter.add(edgeColor);
        m_vis.putAction("filter", filter);
        
        // animated transition
        ActionList animate = new ActionList(1000);
        animate.setPacingFunction(new SlowInSlowOutPacer());
        animate.add(autoPan);
        animate.add(new QualityControlAnimator());
        animate.add(new VisibilityAnimator(tree));
        animate.add(new LocationAnimator(treeNodes));
        animate.add(new ColorAnimator(treeNodes));
        animate.add(new RepaintAction());
        m_vis.putAction("animate", animate);
        m_vis.alwaysRunAfter("filter", "animate");
        
        // create animator for orientation changes
        ActionList orient = new ActionList(2000);
        orient.setPacingFunction(new SlowInSlowOutPacer());
        orient.add(autoPan);
        orient.add(new QualityControlAnimator());
        orient.add(new LocationAnimator(treeNodes));
        orient.add(new RepaintAction());
        m_vis.putAction("orient", orient);
        
        // ------------------------------------------------
        
        // initialize the display
        setSize(700,600);
        setItemSorter(new TreeDepthItemSorter());
        addControlListener(new ControlAdapter(){
        	
        	public void itemClicked(VisualItem item, MouseEvent arg1) {
        		if ( item.canGetString(LABEL) ){
        			//LogError.save(this.getClass(),item.getString(LABEL));        			
        		}
        		
        	}
        });
        addControlListener(new ZoomToFitControl());
        addControlListener(new ZoomControl());
        addControlListener(new WheelZoomControl());
        addControlListener(new PanControl());
        addControlListener(new FocusControl(1, "filter"));
        
        registerKeyboardAction(
            new OrientAction(Constants.ORIENT_LEFT_RIGHT),
            "left-to-right", KeyStroke.getKeyStroke("ctrl 1"), WHEN_FOCUSED);
        registerKeyboardAction(
            new OrientAction(Constants.ORIENT_TOP_BOTTOM),
            "top-to-bottom", KeyStroke.getKeyStroke("ctrl 2"), WHEN_FOCUSED);
        registerKeyboardAction(
            new OrientAction(Constants.ORIENT_RIGHT_LEFT),
            "right-to-left", KeyStroke.getKeyStroke("ctrl 3"), WHEN_FOCUSED);
        registerKeyboardAction(
            new OrientAction(Constants.ORIENT_BOTTOM_TOP),
            "bottom-to-top", KeyStroke.getKeyStroke("ctrl 4"), WHEN_FOCUSED);
        
        // ------------------------------------------------
        
        // filter graph and perform layout
        setOrientation(m_orientation);
        m_vis.run("filter");
        
        TupleSet search = new PrefixSearchTupleSet(); 
        m_vis.addFocusGroup(Visualization.SEARCH_ITEMS, search);
        search.addTupleSetListener(new TupleSetListener() {
            public void tupleSetChanged(TupleSet t, Tuple[] add, Tuple[] rem) {
                m_vis.cancel("animatePaint");
                m_vis.run("fullPaint");
                m_vis.run("animatePaint");
            }
        });
    }
    
    // ------------------------------------------------------------------------
    
    public void setOrientation(int orientation) {
        NodeLinkTreeLayout rtl 
            = (NodeLinkTreeLayout)m_vis.getAction("treeLayout");
        CollapsedSubtreeLayout stl
            = (CollapsedSubtreeLayout)m_vis.getAction("subLayout");
        switch ( orientation ) {
        case Constants.ORIENT_LEFT_RIGHT:
            m_nodeRenderer.setHorizontalAlignment(Constants.LEFT);
            m_edgeRenderer.setHorizontalAlignment1(Constants.RIGHT);
            m_edgeRenderer.setHorizontalAlignment2(Constants.LEFT);
            m_edgeRenderer.setVerticalAlignment1(Constants.CENTER);
            m_edgeRenderer.setVerticalAlignment2(Constants.CENTER);
            break;
        case Constants.ORIENT_RIGHT_LEFT:
            m_nodeRenderer.setHorizontalAlignment(Constants.RIGHT);
            m_edgeRenderer.setHorizontalAlignment1(Constants.LEFT);
            m_edgeRenderer.setHorizontalAlignment2(Constants.RIGHT);
            m_edgeRenderer.setVerticalAlignment1(Constants.CENTER);
            m_edgeRenderer.setVerticalAlignment2(Constants.CENTER);
            break;
        case Constants.ORIENT_TOP_BOTTOM:
            m_nodeRenderer.setHorizontalAlignment(Constants.CENTER);
            m_edgeRenderer.setHorizontalAlignment1(Constants.CENTER);
            m_edgeRenderer.setHorizontalAlignment2(Constants.CENTER);
            m_edgeRenderer.setVerticalAlignment1(Constants.BOTTOM);
            m_edgeRenderer.setVerticalAlignment2(Constants.TOP);
            break;
        case Constants.ORIENT_BOTTOM_TOP:
            m_nodeRenderer.setHorizontalAlignment(Constants.CENTER);
            m_edgeRenderer.setHorizontalAlignment1(Constants.CENTER);
            m_edgeRenderer.setHorizontalAlignment2(Constants.CENTER);
            m_edgeRenderer.setVerticalAlignment1(Constants.TOP);
            m_edgeRenderer.setVerticalAlignment2(Constants.BOTTOM);
            break;
        default:
            throw new IllegalArgumentException(
                "Unrecognized orientation value: "+orientation);
        }
        m_orientation = orientation;
        rtl.setOrientation(orientation);
        stl.setOrientation(orientation);
        
        addControlListener(new ControlAdapter(){
        	public void itemClicked(VisualItem item, MouseEvent e){
        		// LogError.save(this.getClass(),item.getString(LABEL));
        		
        	}
        	
        });
        
    }
    
    public int getOrientation() {
        return m_orientation;
    }
    
    // ------------------------------------------------------------------------
    
    

    public static Tree getTree(int b, int d1, int d2) {
        Tree t = new Tree();
        t.getNodeTable().addColumns(LABEL_SCHEMA);
 
        Node root = t.addRoot();
        root.setString(LABEL, "schemas");
        
        for (ISchema schema : Kernel.getCatalog().getShemas()) {
        	Node nodeSchema = t.addChild(root);
            nodeSchema.setString(LABEL, schema.getName());
            
            
            DBConnection tempConnection = Kernel.getTransactionManager().getConnectionService().getSystemConnection(schema.getName());
            ITransaction transaction = Kernel.getExecuteTransactions().begin(tempConnection, false, false);
            
            Node nodeTables = t.addChild(nodeSchema);
            nodeTables.setString(LABEL, "tables");
           
  
     
           for (ITable table : schema.getTables()) {
        	   
        	   
            	if(!table.isTemp()){
            		  final Node nodeTable = t.addChild(nodeTables); 
                      nodeTable.setString(LABEL, table.getName());     
                   
                      
                      Node nodeBlocks = t.addChild(nodeTable);
                      nodeBlocks.setString(LABEL, "blocks");
                      
                      Node nodeMetadata = t.addChild(nodeTable);
                      nodeMetadata.setString(LABEL, "metadata");
                   
                      
                      //
                      Node nodeTableID = t.addChild(nodeMetadata);
                      nodeTableID.setString(LABEL, "id: " + table.getTableID());
     
                      Node nodeNumberOfBlocks = t.addChild(nodeMetadata);
                      nodeNumberOfBlocks.setString(LABEL, "number of blocks: " + table.getNumberOfBlocks(transaction));
                      
                      Node nodeNumberOfTuples = t.addChild(nodeMetadata);
                      nodeNumberOfTuples.setString(LABEL, "number of tuples: " + table.getNumberOfTuples(transaction));
                      
                      Node nodeStructure = t.addChild(nodeMetadata);
                      nodeStructure.setString(LABEL, "columns: ");
                      
                      for (Column col : table.getColumns()) {
                    	  Node column = t.addChild(nodeStructure);
                    	  column.setString(LABEL, col.getName() + "  " + col.getType());
                      }
                   
                      // 
                      
                    
                      
                      try {
                    	  final CountDownLatch latch = new CountDownLatch(1);
			  
                    	  transaction.execRunnable(new TransactionRunnable() {
          					
          					@Override
          					public void run(ITransaction transaction) {
          						
          						TableScan tableScan = new TableScan(transaction, table); 
          						tableScan.reset();
          						BlockScan block = null;
          						ITuple tuple = null;
          						while((block = tableScan.nextBlock()) != null ){
          							
          							int count = 0;
          							Node nodeBlock = null;
          							Node nodeTuples = null;
          							while((tuple = block.nextTuple())!=null){ 
          								
          								if(nodeBlock == null){
          	    							nodeBlock = t.addChild(nodeBlocks); 
          	    							nodeBlock.setString(LABEL, "id: " + block.getAtualBlock());  
          	    							
          	    							nodeTuples = t.addChild(nodeBlock);
          	    		                    nodeTuples.setString(LABEL, "tuples");
          	    		                   // nodeTuples.setString(LABEL2, "1-2-3-4");
          	    		                   
          	    							
          								}
          								count++;
          								Node nodeTuple = t.addChild(nodeTuples); 
          								nodeTuple.setString(LABEL, "id: " + tuple.getId());
          								
          						
          								Node nodeTupleData = t.addChild(nodeTuple); 
          								nodeTupleData.setString(LABEL, tuple.getStringData());     
          								if(count==10){
          									break;
          								}
          							}
          						}
          						
          					
          						latch.countDown();
          					}
          					
          					@Override
          					public void onFail(ITransaction transaction, Exception e) {
          						latch.countDown();
          						
          					}
          				});
                    	  
                  
                    	  latch.await();
                      } catch (InterruptedException e1) {
						// TODO Auto-generated catch block
                    		Kernel.exception(DrawSchemaNavigator.class,e1);
					}
            	}
			}
		}

       
        return t;
    }
    
    
    public static JComponent create() {
        Color BACKGROUND = Color.WHITE;
        Color FOREGROUND = Color.BLACK;
        
        Tree t = null;
        try {
            //t = (Tree)new TreeMLReader().readGraph(datafile);
        	t = getTree(20, 20, 20);
        } catch ( Exception e ) {
        	Kernel.exception(DrawSchemaNavigator.class,e);
           // System.exit(1);
        }
        
        // create a new treemap
        final  DrawSchemaNavigator tview = new  DrawSchemaNavigator(t);
        tview.setBackground(BACKGROUND);
        tview.setForeground(FOREGROUND);
        
       
        final JFastLabel title = new JFastLabel("               ");
        title.setPreferredSize(new Dimension(350, 20));
        title.setVerticalAlignment(SwingConstants.BOTTOM);
        title.setBorder(BorderFactory.createEmptyBorder(3,0,0,0));
        title.setFont(FontLib.getFont("Tahoma", Font.PLAIN, 16));
        title.setBackground(BACKGROUND);
        title.setForeground(FOREGROUND);
        
//        tview.addControlListener(new ControlAdapter() {
//            public void itemEntered(VisualItem item, MouseEvent e) {
//                if ( item.canGetString(LABEL) )
//                    title.setText(item.getString(LABEL));
//            }
//            public void itemExited(VisualItem item, MouseEvent e) {
//                title.setText(null);
//            }
//        });
        
        Box box = new Box(BoxLayout.X_AXIS);
        box.add(Box.createHorizontalStrut(10));
        box.add(title);
        box.add(Box.createHorizontalGlue());
        box.add(Box.createHorizontalStrut(3));
        box.setBackground(BACKGROUND);
        
        JPanel infos = new JPanel(new BorderLayout());
   

     
        JPanel panel = new JPanel(new BorderLayout());
        tview.setBorder(new LineBorder(Color.black));
        panel.setBackground(BACKGROUND);
        panel.setForeground(FOREGROUND);
        panel.add(tview, BorderLayout.CENTER);
        panel.add(box, BorderLayout.SOUTH);
        panel.add(infos,BorderLayout.NORTH);
        return panel;
    }
    
    // ------------------------------------------------------------------------
   
    public class OrientAction extends AbstractAction {

		private static final long serialVersionUID = 1L;
		private int orientation;
        
        public OrientAction(int orientation) {
            this.orientation = orientation;
        }
        public void actionPerformed(ActionEvent evt) {
            setOrientation(orientation);
            getVisualization().cancel("orient");
            getVisualization().run("treeLayout");
            getVisualization().run("orient");
        }
    }
    
    public class AutoPanAction extends Action {
        private Point2D m_start = new Point2D.Double();
        private Point2D m_end   = new Point2D.Double();
        private Point2D m_cur   = new Point2D.Double();
        private int     m_bias  = 150;
        
        public void run(double frac) {
            TupleSet ts = m_vis.getFocusGroup(Visualization.FOCUS_ITEMS);
            if ( ts.getTupleCount() == 0 )
                return;
            
            if ( frac == 0.0 ) {
                int xbias=0, ybias=0;
                switch ( m_orientation ) {
                case Constants.ORIENT_LEFT_RIGHT:
                    xbias = m_bias;
                    break;
                case Constants.ORIENT_RIGHT_LEFT:
                    xbias = -m_bias;
                    break;
                case Constants.ORIENT_TOP_BOTTOM:
                    ybias = m_bias;
                    break;
                case Constants.ORIENT_BOTTOM_TOP:
                    ybias = -m_bias;
                    break;
                }

                VisualItem vi = (VisualItem)ts.tuples().next();
                m_cur.setLocation(getWidth()/2, getHeight()/2);
                getAbsoluteCoordinate(m_cur, m_start);
                m_end.setLocation(vi.getX()+xbias, vi.getY()+ybias);
            } else {
                m_cur.setLocation(m_start.getX() + frac*(m_end.getX()-m_start.getX()),
                                  m_start.getY() + frac*(m_end.getY()-m_start.getY()));
                panToAbs(m_cur);
            }
        }
    }
    
    public static class NodeColorAction extends ColorAction {
        
        public NodeColorAction(String group) {
            super(group, VisualItem.FILLCOLOR);
        }
        
        public int getColor(VisualItem item) {
            if ( m_vis.isInGroup(item, Visualization.SEARCH_ITEMS) )
                return ColorLib.rgb(255,190,190);
            else if ( m_vis.isInGroup(item, Visualization.FOCUS_ITEMS) )
                return ColorLib.rgb(198,229,229);
            else if ( item.getDOI() > -1 )
                return ColorLib.rgb(164,193,193);
            else
                return ColorLib.rgba(255,255,255,0);
        }
        
    }
    
}
