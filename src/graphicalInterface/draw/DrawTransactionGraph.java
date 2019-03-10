package graphicalInterface.draw;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.SwingConstants;
import javax.swing.border.LineBorder;

import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxGraphLayout;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.util.mxConstants;
import com.mxgraph.util.mxUtils;
import com.mxgraph.view.mxGraph;
import com.mxgraph.view.mxStylesheet;

import DBMS.Kernel;
import DBMS.transactionManager.TransactionManagerListener;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.Lock;
import DBMS.transactionManager.TransactionOperation;
import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.Insets;




public class DrawTransactionGraph extends JPanel implements TransactionManagerListener {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private mxGraph graph;
	private mxGraphComponent graphComponent;
	private JScrollPane leftScrollPanel;
	private JPanel transactionsPanel;
	private HashMap<String, Object> vertexMap;
	private LinkedList<DrawTransaction> drawTransactions;
	private JPanel scheduledListPanel;
	private List<Registry> registries;
	private JPanel lockListPanel;
	//private int rows = 0;
	
	private final String VERTEX_STYLE = mxConstants.STYLE_SHAPE + "=" + mxConstants.SHAPE_ELLIPSE + ";"
			+ JGraphStyle.STROKECOLOR.mxStyle + ";" + JGraphStyle.FONTCOLOR.mxStyle + ";"
			+ JGraphStyle.FONTSTYLE.mxStyle + ";" + JGraphStyle.FILLCOLOR.mxStyle;

	public DrawTransactionGraph(Dimension dimension) {

		this.setLayout(new GridLayout(1, 1));

		drawTransactions();
		drawGraph(dimension);
		applyEdgeDefaults();
		vertexMap = new HashMap<>();
		
		JSplitPane rightPanel = new JSplitPane();
		rightPanel.setOrientation(JSplitPane.VERTICAL_SPLIT);
	
		
		JPanel schedulePanel = createSchedulePanel();
	
		JPanel graphPanel = createGraphPanel();
		
		JPanel lockPanel = createLockPanel();
		
		
		
		JSplitPane jSplitPaneGraphAndLock = new JSplitPane();
		jSplitPaneGraphAndLock.setOrientation(JSplitPane.HORIZONTAL_SPLIT);
		jSplitPaneGraphAndLock.setLeftComponent(graphPanel);
		jSplitPaneGraphAndLock.setRightComponent(lockPanel);
		jSplitPaneGraphAndLock.setResizeWeight(0.5);
		
		
		JScrollPane jsp = new JScrollPane();
		GridBagConstraints gbc_jsp = new GridBagConstraints();
		gbc_jsp.fill = GridBagConstraints.BOTH;
		gbc_jsp.gridx = 0;
		gbc_jsp.gridy = 1;
		schedulePanel.add(jsp, gbc_jsp);
		
		rightPanel.setLeftComponent(jSplitPaneGraphAndLock);
		rightPanel.setRightComponent(schedulePanel);
		
		scheduledListPanel = new JPanel();
		jsp.setViewportView(scheduledListPanel);
		scheduledListPanel.setLayout(new GridLayout(1,80));
		scheduledListPanel.setBackground(Color.lightGray);
		rightPanel.setResizeWeight(0.8);
		
		
		JSplitPane jSplitPane = new JSplitPane();
		jSplitPane.setLeftComponent(leftScrollPanel);
		jSplitPane.setRightComponent(rightPanel);
		jSplitPane.setResizeWeight(0.8);
		this.add(jSplitPane);
		Kernel.setTransactionManagerListener(this);
		
		registries = new ArrayList<>();
	}


	private JPanel createLockPanel() {
		JPanel lockPanel = new JPanel();
		lockPanel.setBackground(Color.white);
		
		GridBagLayout gbl_lockPanel = new GridBagLayout();
		gbl_lockPanel.rowHeights = new int[]{30, 0, 0};
		gbl_lockPanel.columnWeights = new double[]{1.0};
		gbl_lockPanel.rowWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
		lockPanel.setLayout(gbl_lockPanel);
		
		GridBagConstraints gbc_labelLock = new GridBagConstraints();
		gbc_labelLock.insets = new Insets(0, 0, 5, 0);
		gbc_labelLock.gridx = 0;
		gbc_labelLock.gridy = 0;
		JLabel labelSchedule = new JLabel("Lock Table",SwingConstants.CENTER);
		lockPanel.add(labelSchedule, gbc_labelLock);
		

		lockListPanel = new JPanel();
		lockListPanel.setBorder(new LineBorder(new Color(0, 0, 0)));
		lockListPanel.setBackground(Color.WHITE);
		lockListPanel.setLayout(new GridLayout(400,1));
		
		JScrollPane jScrollPane = new JScrollPane(lockListPanel);
		
		GridBagConstraints gbc_lockList = new GridBagConstraints();
		gbc_lockList.fill = GridBagConstraints.BOTH;
		gbc_lockList.gridx = 0;
		gbc_lockList.gridy = 1;
		lockPanel.add(jScrollPane, gbc_lockList);
		
		return lockPanel;
	}
	

	private JPanel createSchedulePanel() {
		JPanel schedulePanel = new JPanel();
		schedulePanel.setBackground(Color.WHITE);
		
		GridBagLayout gbl_schedulePanel = new GridBagLayout();
		gbl_schedulePanel.rowHeights = new int[]{30, 0, 0};
		gbl_schedulePanel.columnWeights = new double[]{1.0};
		gbl_schedulePanel.rowWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
		schedulePanel.setLayout(gbl_schedulePanel);
		
		GridBagConstraints gbc_labelSchedule = new GridBagConstraints();
		gbc_labelSchedule.insets = new Insets(0, 0, 5, 0);
		gbc_labelSchedule.gridx = 0;
		gbc_labelSchedule.gridy = 0;
		JLabel labelSchedule = new JLabel("Schedule",SwingConstants.CENTER);
		schedulePanel.add(labelSchedule, gbc_labelSchedule);
		
		return schedulePanel;
	}

	private JPanel createGraphPanel() {
		JPanel graphPanel = new JPanel();
		graphPanel.setBackground(Color.WHITE);
		GridBagLayout gbl_graphPanel = new GridBagLayout();
		gbl_graphPanel.rowHeights = new int[]{30, 0, 0};
		gbl_graphPanel.columnWeights = new double[]{1.0};
		gbl_graphPanel.rowWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
		graphPanel.setLayout(gbl_graphPanel);
		
		GridBagConstraints gbc_labelGraph = new GridBagConstraints();
		gbc_labelGraph.insets = new Insets(0, 0, 5, 0);
		gbc_labelGraph.gridx = 0;
		gbc_labelGraph.gridy = 0;
		JLabel labelGraph = new JLabel("Serialization Graph",SwingConstants.CENTER);
		graphPanel.add(labelGraph ,gbc_labelGraph);
		
		GridBagConstraints gbc_graph = new GridBagConstraints();
		gbc_graph.fill = GridBagConstraints.BOTH;
		gbc_graph.gridx = 0;
		gbc_graph.gridy = 1;
		graphPanel.add(graphComponent, gbc_graph);
		return graphPanel;
	}

	private void layoutGraph() {
		mxGraphLayout layout = new mxCircleLayout(graph);
		Object cell = graph.getDefaultParent();
		graph.getModel().beginUpdate();
		try {
			layout.execute(cell);
		} finally {
			graph.getModel().endUpdate();
		}
	}

	public void drawTransactions() {
		drawTransactions = new LinkedList<>();
		transactionsPanel = new JPanel();
		transactionsPanel.setLayout(new GridLayout(1, 80));
		leftScrollPanel = new JScrollPane(transactionsPanel);
		
	}

	public void drawGraph(Dimension dimension) {
		graph = new mxGraph();
		graph.setCellsEditable(false);
		graph.setConnectableEdges(false);
		graph.setDisconnectOnMove(false);
		graphComponent = new mxGraphComponent(graph);
		graphComponent.setConnectable(false);
		graphComponent.setPreferredSize(new Dimension((int) dimension.getWidth() / 2, (int) dimension.getHeight()));
		graphComponent.getViewport().setOpaque(true);
		graphComponent.getViewport().setBackground(Color.WHITE);
	}

	public void drawVertex(String name) {
		if (!vertexMap.containsKey(name)) {
			graph.getModel().beginUpdate();
			Object parent = graph.getDefaultParent();
			Object v = graph.insertVertex(parent, null, name, 0, 0, 70, 70, VERTEX_STYLE);
			graph.getModel().endUpdate();
			vertexMap.put(name, v);
			layoutGraph();
			
		}
		

	}

	public void drawEdge(String v1, String v2) {

		Object vO1 = vertexMap.get(v1);
		Object vO2 = vertexMap.get(v2);
		if (vO1 != null && vO2 != null) {
			Object parent = graph.getDefaultParent();
			graph.getModel().beginUpdate();
			graph.insertEdge(parent, null, "", vO1, vO2);
			graph.getModel().endUpdate();

		}

	}

	public void removeVertex(String vetex) {
		Object v = vertexMap.remove(vetex);
		if (v != null) {
			graph.removeCells(new Object[] { v });
		}
	}

	private void applyEdgeDefaults() {

		Map<String, Object> edge = new HashMap<String, Object>();

		edge.put(mxConstants.STYLE_ROUNDED, true);
		edge.put(mxConstants.STYLE_STROKECOLOR, "#000000"); // default is
															// #6482B9
		edge.put(mxConstants.STYLE_FONTCOLOR, "#446299");
		edge.put(mxConstants.STYLE_SHAPE, mxConstants.SHAPE_CONNECTOR);
		edge.put(mxConstants.STYLE_EDGE, mxConstants.EDGESTYLE_SIDETOSIDE);
		edge.put(mxConstants.STYLE_ORTHOGONAL, false);
		edge.put(mxConstants.STYLE_ENDARROW, mxConstants.ARROW_CLASSIC);
		edge.put(mxConstants.STYLE_VERTICAL_ALIGN, mxConstants.ALIGN_MIDDLE);
		edge.put(mxConstants.STYLE_ALIGN, mxConstants.ALIGN_CENTER);

		mxStylesheet edgeStyle = new mxStylesheet();
		edgeStyle.setDefaultEdgeStyle(edge);
		graph.setStylesheet(edgeStyle);

	}
	

	
	
	
	public synchronized void newTransactionOperationScheduled(TransactionOperation transactionOperation) {
		
		
		if(registries.size() >= 50){
			
			Registry registry = registries.remove(0);
			scheduledListPanel.remove(registry);
			
			registry = createRegistry(transactionOperation, registry);
			
			registries.add(registry);
			scheduledListPanel.add(registry);
			
		}else{
			Registry registry = createRegistry(transactionOperation, new Registry());
			
			registries.add(registry);
			scheduledListPanel.add(registry);
		}
		
		scheduledListPanel.revalidate();
		scheduledListPanel.repaint();
	
	}
	
	public synchronized void newTransactionScheduledCommitOrAbort(ITransaction t, boolean isCommited) {
	
		if(registries.size() >= 50){
			
			Registry registry = registries.remove(0);
			scheduledListPanel.remove(registry);
			
			registry = createRegistryCommitOrAbort(t, registry, isCommited);
			
			registries.add(registry);
			scheduledListPanel.add(registry);
			
		}else{
			Registry registry = createRegistryCommitOrAbort(t, new Registry(), isCommited);
			
			registries.add(registry);
			scheduledListPanel.add(registry);
		}
		
		scheduledListPanel.revalidate();
		scheduledListPanel.repaint();
		
		
	
		
	}
	
	
	public Registry createRegistry(TransactionOperation transactionOperation, Registry registry){

		registry.setLayout(new GridLayout(1, 1));
		registry.setPreferredSize(new Dimension(100, 60));
		registry.setMinimumSize(new Dimension(100, 60));
		registry.setMinimumSize(new Dimension(100, 60));
		
		JLabel text = null;
		
		if(registry.l == null){
			text = new JLabel();
			text.setHorizontalAlignment(SwingConstants.CENTER);
			text.setVerticalAlignment(SwingConstants.CENTER);
			text.setBorder(BorderFactory.createLineBorder(Color.black));
			text.setOpaque(true);
			registry.l = text;			
			registry.add(text);
		}else{
			text = registry.l;
			
		}
				
		switch (transactionOperation.getType()) {
		case TransactionOperation.READ_TRANSACTION:
			text.setBackground(DrawPage.READ_COLOR);
			text.setText("T"+transactionOperation.getTransaction().getIdT() + " R("+transactionOperation.getObjectDatabaseId()+")");
			break;
		case TransactionOperation.WRITE_TRANSACTION:
			text.setBackground(DrawPage.WRITE_COLOR);
			text.setText("T"+transactionOperation.getTransaction().getIdT() + " W("+transactionOperation.getObjectDatabaseId()+")");
			break;
		default:
			break;
		}
		return registry;
	}
	
	public Registry createRegistryCommitOrAbort(ITransaction t, Registry registry, boolean isCommited){

		registry.setLayout(new GridLayout(1, 1));
		registry.setPreferredSize(new Dimension(100, 60));
		registry.setMinimumSize(new Dimension(100, 60));
		registry.setMinimumSize(new Dimension(100, 60));
		
		JLabel text = null;
		
		if(registry.l == null){
			text = new JLabel();
			text.setHorizontalAlignment(SwingConstants.CENTER);
			text.setVerticalAlignment(SwingConstants.CENTER);
			text.setBorder(BorderFactory.createLineBorder(Color.black));
			text.setOpaque(true);
			registry.l = text;			
			registry.add(text);
		}else{
			text = registry.l;
			
		}
				
		if(isCommited){
			text.setBackground(Color.GREEN);
			text.setText("Commit T " +t.getIdT());
		}else{
			text.setBackground(Color.RED);
			text.setText("Abort T " +t.getIdT());
		}
		
		
		return registry;
	}
	
	
	class Registry extends JPanel{

		private static final long serialVersionUID = 1L;
		JLabel l;
	}
	
	
	

	public DrawTransaction appendDrawTransaction(ITransaction transaction, String string) {
	//	rows++;
		for (DrawTransaction drawTransaction : drawTransactions) {
			if (drawTransaction.getTransaction() == transaction){
				
				drawTransaction.appendOperation(string);
				
			}else{
				///drawTransaction.appendOperation("-");
			}
		}
		return null;
	}

	@Override
	public void newTransaction(ITransaction transaction) {
		drawVertex("T" + transaction.getIdT());
		DrawTransaction drawTransaction = new DrawTransaction(transaction, this);
		transactionsPanel.add(drawTransaction);
		drawTransactions.add(drawTransaction);
		/*
		for (int i = 0; i < rows; i++) {
			drawTransaction.appendOperation("-");
		}
		*/
		leftScrollPanel.getHorizontalScrollBar().setValue(leftScrollPanel.getHorizontalScrollBar().getMaximum());
		leftScrollPanel.revalidate();
		

	}
	
	public void removeTransaction(DrawTransaction drawTransaction){
		transactionsPanel.remove(drawTransaction);
		drawTransactions.remove(drawTransaction);
		transactionsPanel.revalidate();
		transactionsPanel.repaint();
	//	if(drawTransactions.isEmpty())rows=0;
	}

	@Override
	public  void newTransactionOperation(TransactionOperation transactionOperation) {

		switch (transactionOperation.getType()) {
		case TransactionOperation.READ_TRANSACTION:
			appendDrawTransaction(transactionOperation.getTransaction(),"Read("+transactionOperation.getObjectDatabaseId()+")");
			break;
		case TransactionOperation.WRITE_TRANSACTION:
			appendDrawTransaction(transactionOperation.getTransaction(),"Write("+transactionOperation.getObjectDatabaseId()+")");
			break;
		default:
			break;
		}
				
	}

	@Override
	public  void transactionCommit(ITransaction transaction) {

		removeVertex("T" + transaction.getIdT());
		appendDrawTransaction(transaction,"Commit");
		newTransactionScheduledCommitOrAbort(transaction,true);
		
	}

	@Override
	public void transactionAbort(ITransaction transaction) {

		removeVertex("T" + transaction.getIdT());
		appendDrawTransaction(transaction,"Abort");
		newTransactionScheduledCommitOrAbort(transaction,false);
	}

	@Override
	public  void transactionFailed(ITransaction transaction) {

		removeVertex("T" + transaction.getIdT());
		appendDrawTransaction(transaction,"Failed");
	}

	@Override
	public  void newGraphEdgeConflit(ITransaction t1, ITransaction t2) {

		drawEdge("T" + t1.getIdT(), "T" + t2.getIdT());

	}
	
	
	private HashMap<String, DrawLock> locksHash = new HashMap<>();
	
	@Override
	public void newLock(Lock lock) {
		// TODO Auto-generated method stub
		if(lock!=null){
			DrawLock l = locksHash.get(lock.getObjectDatabaseId().toString());
			if(l == null){
				l = new DrawLock();
				l.update(lock);
				locksHash.put(lock.getObjectDatabaseId().toString(), l);
				lockListPanel.add(l);
				lockListPanel.revalidate();
				lockListPanel.repaint();
			}else{
				//LogError.save(this.getClass(),"[ERRO] Interface lock IDObj: " + lock.getObjectDatabaseId() + " IDT: " + lock.getTransaction());
			}			
		}
	}


	@Override
	public void unLock(Lock lock) {
		DrawLock l = locksHash.remove(lock.getObjectDatabaseId().toString());
		if(l!=null){
			lockListPanel.remove(l);
			lockListPanel.revalidate();
			lockListPanel.repaint();
		}
	}



	@Override
	public void updateLock(Lock lock) {
		DrawLock l = locksHash.get(lock.getObjectDatabaseId().toString());
		if(l!=null){
			l.update(lock);
		}
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








}