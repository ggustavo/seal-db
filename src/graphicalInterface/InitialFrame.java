package graphicalInterface;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.sql.SQLException;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSlider;
import javax.swing.JTabbedPane;
import javax.swing.JTextPane;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ExecuteTransactions;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SubplanOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawBuffer;
import graphicalInterface.draw.DrawJoin;
import graphicalInterface.draw.DrawPlan;
import graphicalInterface.draw.DrawRecovery;
import graphicalInterface.draw.DrawRelationalCalculus;
import graphicalInterface.draw.DrawTable;
import graphicalInterface.draw.DrawTransactionGraph;
import graphicalInterface.images.ImagensController;
import graphicalInterface.menus.mainMenus.OperationsMenu;
import graphicalInterface.menus.mainMenus.OptionsOperationsMenu;
import graphicalInterface.util.ClosableTabbedPane;
import graphicalInterface.util.CloseTabbeListener;
import graphicalInterface.util.JSplitPaneMultiTabs;
import graphicalInterface.util.SQLHighlighter;



public class InitialFrame extends JFrame {

	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	
	private Dimension screenSize;
	private ClosableTabbedPane joinLayers;
	private ClosableTabbedPane planLayers;
	private JFrame frame;
	private JFrame transactionFrame;
	private JFrame bufferFrame;
	private JFrame recoveryFrame;
	

	public void setScreamSize() {
		Toolkit toolkit = Toolkit.getDefaultToolkit();

		screenSize = toolkit.getScreenSize();
		// LogError.save(this.getClass(),"Physical screen size: " + screenSize);

		Insets insets = toolkit.getScreenInsets(getGraphicsConfiguration());
		// LogError.save(this.getClass(),"Insets: " + insets);

		screenSize.width -= (insets.left + insets.right);
		screenSize.height -= (insets.top + insets.bottom);
		// LogError.save(this.getClass(),"Max available: " + screenSize);

	}
	
	public Dimension calcIdealDimession(){
		Dimension d = new Dimension();
		d.height = screenSize.height - (int)(screenSize.height / 6);
		d.width = screenSize.width - (int)(screenSize.width / 10);
		this.setLocation(screenSize.width-d.width, 0);
		return d;
	}
	
	public void createJoinFrame(ClosableTabbedPane joinLayers){
		frame = new JFrame();
		frame.setIconImage(ImagensController.FRAME_ICON_JOIN);
		frame.addWindowListener(new WindowListener() {
			
			public void windowOpened(WindowEvent e){}
			public void windowIconified(WindowEvent e){}
			public void windowDeiconified(WindowEvent e){}
			public void windowDeactivated(WindowEvent e) {}
			public void windowClosed(WindowEvent e) {}
			public void windowActivated(WindowEvent e) {}
			public void windowClosing(WindowEvent e) {
				
				frame.setVisible(false);	
			}
		});
		
		
		frame.setTitle("Join View");
		frame.setSize(calcIdealDimession());
		frame.setResizable(true);
		frame.setBackground(Color.blue);
		frame.setVisible(false);
		frame.add(joinLayers);
		
	}

	
	
	
	public InitialFrame() {
		
		//JFrame.setDefaultLookAndFeelDecorated(true);
		InterfaceSystem();		
		setScreamSize();
		showTransactionFrame();
		showBufferFrame();
		showRecoveryFrame();
		
		this.setIconImage(ImagensController.FRAME_ICON_DATABASE);
		

		this.setTitle("Query Processing");
		this.setSize(calcIdealDimession());
		this.setResizable(true);
		this.setBackground(Color.blue);

		planLayers = new ClosableTabbedPane();
		planLayers.setBackground(Color.YELLOW);
		this.add(planLayers);

		//Events.panelArea = (JPanel) this.getContentPane();
		this.setVisible(false);

		
		joinLayers = new ClosableTabbedPane();
		createJoinFrame(joinLayers);
		
		

	}
	

	public void showSubPlanEditor(DBConnection connection, Plan plan, SubplanOperation subplanOperation){
		JFrame frame = new JFrame();
		frame.setIconImage(ImagensController.FRAME_ICON_SUBPLAN);
		
		frame.setSize(calcIdealDimession());
		frame.setTitle("Subplan Editor");
		frame.setResizable(true);
		frame.setBackground(Color.blue);
		frame.setVisible(true);
	
		JPanel areaMouse = new JPanel();
		
		OptionsOperationsMenu menuOptions = new OptionsOperationsMenu(connection);
		menuOptions.setBackground(Color.lightGray);
	
		OperationsMenu planOperationsMenu = new OperationsMenu(areaMouse);

		final DrawPlan drawPlan = new DrawPlan(menuOptions,this,areaMouse);
		drawPlan.drawPlan(plan);
	
		drawPlan.setPreferredSize(new Dimension(screenSize.width, screenSize.height));
		
		JPanel exl = new JPanel();
		exl.setBackground(Color.LIGHT_GRAY);
		exl.setLayout(new BoxLayout(exl, BoxLayout.Y_AXIS));
		JButton save = new JButton("  Save Subplan  ");
		JPanel auxPanel = new JPanel(new BorderLayout());
		auxPanel.setBackground(Color.white);
		auxPanel.setBorder(BorderFactory.createLineBorder(Color.black));
		auxPanel.add(Box.createRigidArea(new Dimension(5,5)),BorderLayout.NORTH);
		auxPanel.add(save,BorderLayout.EAST);
		auxPanel.add(Box.createRigidArea(new Dimension(5,5)),BorderLayout.SOUTH);

		
		save.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				closeSubPlan(plan, subplanOperation, drawPlan);
			}

		});
		
		exl.add(drawPlan);
		exl.add(auxPanel);
		
		JSplitPaneMultiTabs planLayeredPane = new JSplitPaneMultiTabs();
		planLayeredPane.addComponent(menuOptions).setResizeWeight(0.11);
		planLayeredPane.addComponent(exl).setResizeWeight(0.89);
		planLayeredPane.addComponent(planOperationsMenu).setResizeWeight(1);
		
		areaMouse.setLayout(new GridLayout(1,1));
		areaMouse.add(planLayeredPane);
		areaMouse.setBorder(BorderFactory.createLineBorder(Color.black));
		frame.setContentPane(areaMouse);
		
		frame.addWindowListener(new WindowListener() {
			
			public void windowOpened(WindowEvent e){}
			public void windowIconified(WindowEvent e){}
			public void windowDeiconified(WindowEvent e){}
			public void windowDeactivated(WindowEvent e) {}
			public void windowClosed(WindowEvent e) {}
			public void windowActivated(WindowEvent e) {}
			public void windowClosing(WindowEvent e) {
				frame.dispose();
				closeSubPlan(plan, subplanOperation, drawPlan);
			}
		});
	}
	
	private void closeSubPlan(Plan plan, SubplanOperation subplanOperation, final DrawPlan drawPlan) {
		plan.setRoot(drawPlan.getPlan().getRoot());
		plan.setLast(drawPlan.getPlan().getLast());	
		plan.setOptionalMessage(drawPlan.getPlan().getOptionalMessage());
		plan.setType(drawPlan.getPlan().getType());
		subplanOperation.setSubplan(drawPlan.getPlan());
	}
	
	
	public void addPlanLayer(DBConnection connection) {
		
		String sql = "SELECT s_acctbal, s_name, n_name, p.partkey, p_mfgr, s_address, s_phone, s_comment \n"+
				"FROM part p, supplier s, partsupp ps, nation n, region r \n"+
				"WHERE \t p.partkey = ps.partkey and \n"+
				"\t s.suppkey = ps.suppkey and \n"+
				"\t s.nationkey = n.nationkey and \n"+
				"\t n.regionkey = r.regionkey and \n"+
				"\t p.p_size = 20 and \n"+
				"\t r.r_name = 'EUROPE' ";
		
		//String sql = "";
		addPlanLayer(connection, sql);
	}
	public void addPlanLayer(DBConnection connection, String sqlExample) {
	
		JTextPane textSQL = new JTextPane(new SQLHighlighter());
		
		JPanel areaMouse = new JPanel();
		
		OptionsOperationsMenu menuOptions = new OptionsOperationsMenu(connection);
		menuOptions.setBackground(Color.lightGray);
	
		OperationsMenu planOperationsMenu = new OperationsMenu(areaMouse);
		
		
		final DrawPlan drawPlan = new DrawPlan(menuOptions,this,areaMouse);
		JSplitPaneMultiTabs planExternalPanel = new JSplitPaneMultiTabs();
		planExternalPanel.setBackground(Color.WHITE);
		//planExternalPanel.setLayout(new GridLayout(1,1));
		drawPlan.setPreferredSize(new Dimension(screenSize.width, screenSize.height));
		planExternalPanel.addComponent(drawPlan).setResizeWeight(0.9);
		

		
		JPanel querySQL = new JPanel();
		querySQL.setBackground(Color.WHITE);
		
		JTabbedPane tabs = new JTabbedPane();
		
		DrawRelationalCalculus queryRC = new DrawRelationalCalculus(connection,textSQL,tabs);

		
		tabs.add(" SQL Query ", querySQL);
		tabs.add(" Relational Calculus Query ", queryRC);
		
		planExternalPanel.addComponent(tabs).setResizeWeight(0.1);
		planExternalPanel.setAlignment(JSplitPaneMultiTabs.VERTICAL_SPLIT);

		//QUERY
	
		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0, 0, 0, 0};
		gbl_panel.columnWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		querySQL.setLayout(gbl_panel);
		
		JScrollPane scrollPane = new JScrollPane();
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.gridheight = 4;
		gbc_scrollPane.gridwidth = 10;
		gbc_scrollPane.insets = new Insets(0, 0, 5, 5);
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		querySQL.add(scrollPane, gbc_scrollPane);
		
		
		textSQL.setText(sqlExample);
		//textArea.setContentType("HTML/plain");
		scrollPane.setViewportView(textSQL);
		
		JButton buildPlan = new JButton("Build plan");
		GridBagConstraints gbc_btnNewButton2 = new GridBagConstraints();
		gbc_btnNewButton2.gridx = 7;
		gbc_btnNewButton2.gridy = 4;
		querySQL.add(buildPlan, gbc_btnNewButton2);
		
		buildPlan.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				
				if(!textSQL.getText().isEmpty()){
					Parse p = new Parse();
					try {
						List<Plan> plans = p.parse(textSQL.getText(),Kernel.getCatalog().getSchemabyName(connection.getSchemaName()) );
						if(plans==null)return;
						Plan lastPlan = plans.get(plans.size()-1);
						if(lastPlan.getType() == Plan.SELECT_TYPE){	
							drawPlan.drawPlan(lastPlan);
						}
					} catch (Exception e1) {
						JOptionPane.showMessageDialog(null, e1.getMessage());
						Kernel.exception(InitialFrame.class,e1);
					}
				}
				
			}
		});
		
		JButton execSQL = new JButton("Execute SQL");
		GridBagConstraints gbc_btnNewButton = new GridBagConstraints();
		gbc_btnNewButton.gridx = 9;
		gbc_btnNewButton.gridy = 4;
		querySQL.add(execSQL, gbc_btnNewButton);
		Kernel.getExecuteTransactions().setAllTransactionErrorsListener(new ExecuteTransactions.AllTransactionErrorsListener() {
			
			public void onFail(ITransaction transaction, Exception e) {
				
				JOptionPane.showMessageDialog(null, e.getMessage());
			}
		});
		
		execSQL.addActionListener(new ActionListener() {
			

			public void actionPerformed(ActionEvent e) {
					
					if(!textSQL.getText().isEmpty()){
						Parse p = new Parse();
						
						
						execQuery(p,textSQL.getText(), Kernel.getCatalog().getSchemabyName(connection.getSchemaName()));
							
						
					}
			}
			
			
			
			
			private void execQuery(Parse p,String sql, ISchema database) {
				if(drawPlan.getTransaction() != null && drawPlan.getTransaction().getState() == ITransaction.WAIT){
					JOptionPane.showMessageDialog(null, "Current transaction in wait state");	
					return;
				}
			
				if(drawPlan.getTransaction() == null || !drawPlan.getTransaction().canExec()){
					drawPlan.setTransaction(Kernel.getExecuteTransactions().begin(connection));
				}
				
				drawPlan.getTransaction().execRunnable(new TransactionRunnable() {
					
					public void run(ITransaction transaction) {
						
						@SuppressWarnings("unused")
						List<Plan> plans;
						try {
								plans = p.parse( sql,database, new Parse.NewPlanListener() {
								
								public void newPlan(Plan plan) {
									if(transaction.canExec()){
										plan.setTransaction(transaction);
										ITable result = plan.execute();
										if(plan.getOptionalMessage() == null){
											if(result==null)return;
											DrawTable ij = new DrawTable(transaction, result);
											ij.reloadMatriz();
										}else{
											JOptionPane.showMessageDialog(null, plan.getOptionalMessage());	
										}
									}else{
										JOptionPane.showMessageDialog(null, "Transaction has already been finalized");	
										
									}
								}
							});
						} catch (SQLException e) {
							JOptionPane.showMessageDialog(null, e.getMessage());	
							Kernel.exception(InitialFrame.class,e);

						}
	
					}

					@Override
					public void onFail(ITransaction transaction, Exception e) {
						transaction.abort();
						drawPlan.setTransaction(null);
						Kernel.exception(InitialFrame.class,e);
					}
				});
			}
			
			
			
		});
		
		//QUERY
		
		JSplitPaneMultiTabs planLayeredPane = new JSplitPaneMultiTabs();
		planLayeredPane.addComponent(menuOptions).setResizeWeight(0.15);
		planLayeredPane.addComponent(planExternalPanel).setResizeWeight(0.85);
		planLayeredPane.addComponent(planOperationsMenu).setResizeWeight(1);
		
		//this.add(planLayeredPane);
		areaMouse.setLayout(new GridLayout(1,1));
		areaMouse.add(planLayeredPane);
		
		String tile = connection.getSchemaName()+" Connection: "+connection.getId() + "  ";
		planLayers.addTab(tile, areaMouse);
		planLayers.setSelectedComponent(areaMouse);
		

		planLayers.setCloseTabbeListener(new CloseTabbeListener() {
			
			@Override
			public void close(String t) {
				
				
				String id = t.split("\\:")[1].trim();
				Kernel.getTransactionManager().getConnectionService().closeConnection(id);
				
				
			}
		});
		

		
		this.setVisible(true);

	}
	
	
        

	
	public void addJoinLayer(DrawJoin drawJoin){
		
		JSplitPaneMultiTabs planLayeredPane = new JSplitPaneMultiTabs();
		
		JSlider tempoSlider = new JSlider(JSlider.VERTICAL, 0 , 2000, 1000);
    	tempoSlider.setToolTipText("Timer");
    	tempoSlider.setBackground(Color.white);
    	tempoSlider.setPreferredSize(new Dimension(80, 400));;
    	tempoSlider.setMajorTickSpacing(1000);
    	tempoSlider.setMinorTickSpacing(100);
    	tempoSlider.setPaintTicks(true);
    	tempoSlider.setPaintLabels(true);
    	tempoSlider.addChangeListener(new ChangeListener() {
			
			@Override
			public void stateChanged(ChangeEvent e) {
				JSlider source = (JSlider)e.getSource();
			    if (!source.getValueIsAdjusting()) {
			        int fps = (int)source.getValue();
			        drawJoin.setWAIT_TIME(fps+1);
			 
			    }
				
			}
		});
		
    	
		JPanel externalJoinPanel = new JPanel();
		externalJoinPanel.setBackground(Color.WHITE);
		externalJoinPanel.setLayout(new FlowLayout());	
		externalJoinPanel.add(drawJoin);
	
		JPanel panel = new JPanel();	
		panel.setBackground(Color.RED);
    	panel.setLayout(new BorderLayout());
		panel.add(externalJoinPanel, BorderLayout.CENTER);	
		planLayeredPane.addComponent( panel).setResizeWeight(0.85);
    	planLayeredPane.addComponent(tempoSlider).setResizeWeight(0.15);
		
    	frame.setVisible(true);
    	joinLayers.addTab("Join  ", planLayeredPane);
		joinLayers.setSelectedComponent(planLayeredPane);
		
	}

	
	public void showBufferFrame(){
		getBufferFrame();
		bufferFrame.setVisible(true);
	}
	
	
	
	public void showRecoveryFrame(){
		getRecoveryFrame();
		recoveryFrame.setVisible(true);
	}
	

	
	public void showTransactionFrame(){
		getTransactionFrame();
		transactionFrame.setVisible(true);
	}
	
	public void hideTransactionFrame(){
		transactionFrame.setVisible(false);
	}
	
	public void hideBufferFrame(){
		bufferFrame.setVisible(false);
	}
	
	public void hideRecoveryFrame(){
		recoveryFrame.setVisible(false);
	}
	

	public void hideNodeConnectionFrame(){
		recoveryFrame.setVisible(false);
	}
	

	
	public JFrame getRecoveryFrame() {
		if(recoveryFrame == null){
			recoveryFrame = new JFrame("Recovery Manager");
			recoveryFrame.setIconImage(ImagensController.FRAME_ICON_RECOVERY);
			recoveryFrame.setSize(screenSize);
			recoveryFrame.setBackground(Color.white);
			recoveryFrame.add(new DrawRecovery());			
		}
		return recoveryFrame;
	}
	
	
	public JFrame getTransactionFrame() {
		if(transactionFrame==null){
			transactionFrame = new JFrame("Transaction Manager");
			transactionFrame.setIconImage(ImagensController.FRAME_ICON_TRANSACTION);
			transactionFrame.setSize(screenSize);
			transactionFrame.setBackground(Color.white);
			transactionFrame.add(new DrawTransactionGraph(screenSize));			
		}
		return transactionFrame;
	}

	public JFrame getBufferFrame() {
		if(bufferFrame == null){
			bufferFrame = new JFrame("Buffer Manager");
			bufferFrame.setIconImage(ImagensController.FRAME_ICON_BUFFER);
			bufferFrame.setSize(screenSize);
			bufferFrame.setBackground(Color.white);
			bufferFrame.add(new DrawBuffer());			
		}
		return bufferFrame;
	}

	public void InterfaceSystem() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (UnsupportedLookAndFeelException e) {

		} catch (ClassNotFoundException e) {

		} catch (InstantiationException e) {

		} catch (IllegalAccessException e) {

		}
	}

}