package graphicalInterface.menus.subMenus;


import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.LinkedList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.FilterOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.draw.DrawTable;
import graphicalInterface.menus.FilterField;


public class FilterMenu extends AbstractSubMenu{

	private static final long serialVersionUID = 1L;
	
	private JComboBox<AbstractPlanOperation> operations;
	private DrawPlanOperation drawPlanOperation; 
	
	private JPanel fieldsPanel;
	private List<FilterField> fields;
	
	public void update(DrawPlanOperation drawPlanOperation){
		
		this.drawPlanOperation = drawPlanOperation;
		
		FilterOperation op = (FilterOperation) drawPlanOperation.getPlanOperation();
		
		AbstractPlanOperation opearationForEche = null;
		operations.removeAllItems();
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		if(drawPlanOperation!=null){
			opearationForEche = op.getForeache();
			if(opearationForEche!=null && op.getOperationsResults().contains(opearationForEche)){
				operations.addItem(opearationForEche);
			}else {
				opearationForEche = null;
			}
		}	
		List<AbstractPlanOperation> operationsList = op.getOperationsResults();
		for (AbstractPlanOperation iOp : operationsList) {
			if ( iOp != opearationForEche) {
				operations.addItem(iOp);
			}
		}
		if(opearationForEche == null) {
			if(!operationsList.isEmpty()) {
				opearationForEche = operationsList.get(0);
				operations.setSelectedItem(opearationForEche);	
				op.setTargetPosition(0);
			}
		}else {
			operations.setSelectedItem(opearationForEche);			
		}
		
		fields.clear();
		fieldsPanel.removeAll();
		if(drawPlanOperation!=null && drawPlanOperation.getPlanOperation().getLeft()!=null){
			List<Condition> aov = drawPlanOperation.getPlanOperation().getAttributesOperatorsValues();
			for (Condition c : aov) {
				
				if(c.getTable2()!=null) {
					try{
						c.setTable2(operationsList.get(Integer.parseInt(c.getTable2())).toString());
					}catch (NumberFormatException e) {
						
					}
				}
			
				new FilterField(drawPlanOperation.getPlanOperation(), fieldsPanel, fields,c.getType(),c);
				
			}
			
		
			fieldsPanel.revalidate();
			fieldsPanel.repaint();
		}
		
	}
	
	
	public FilterMenu(DBConnection connection) {
		super(connection);
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		this.setBackground(Color.white);
		this.setLayout(new BorderLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		operations = new JComboBox<AbstractPlanOperation>();
		operations.setBorder(null);
		operations.setRenderer(new DefaultListCellRenderer() {
	
			private static final long serialVersionUID = 1L;

			@Override
		    public Component getListCellRendererComponent(JList<?> list, Object value,
		            int index, boolean isSelected, boolean cellHasFocus) {
		         JComponent component = (JComponent) super.getListCellRendererComponent(list, value, index, isSelected,
		                cellHasFocus);
		         String tip = null;
		         if (value instanceof AbstractPlanOperation) {
		             tip = ((AbstractPlanOperation) value).getName();
		         }
		         list.setToolTipText(tip);
		         return component;
		    }
		});
		
		operations.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent arg0) {
				apply();
				update(drawPlanOperation);
				
			}
		});
		
		fieldsPanel = new JPanel();
		fieldsPanel.setLayout(new GridLayout(50, 1));
		fieldsPanel.setBackground(Color.WHITE);
		fieldsPanel.setBorder(BorderFactory.createLineBorder(Color.black));
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.getViewport().add(fieldsPanel);
		
		fields = new LinkedList<FilterField>();
		
		JLabel columnLabel = new JLabel("  Filters:  ");//Column: 
		
		
		JButton applyButton = new JButton("Apply");
		applyButton.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				apply();
			}
		});
		JButton execute = new JButton("Execute");
		execute.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				apply();
				execute();
			}
		});

		JButton removeOPButton = new JButton("Remove Operation");
		removeOPButton.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				removeFromPlan(drawPlanOperation);

			}
		});
		
		
		JButton columns = new JButton("Add Column-Column Filter");
		columns.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				new FilterField(drawPlanOperation.getPlanOperation(), fieldsPanel, fields,Condition.COLUMN_COLUMN,null);
				
				
				fieldsPanel.revalidate();
				fieldsPanel.repaint();

			}
		});
		
		JButton value = new JButton("Add Column-Value Filter");
		value.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				new FilterField(drawPlanOperation.getPlanOperation(), fieldsPanel, fields,Condition.COLUMN_VALUE,null);
				
				fieldsPanel.revalidate();
				fieldsPanel.repaint();

			}
		});
		
		JButton correlation = new JButton("Add Correlation Filter");
		correlation.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

			
				new FilterField(drawPlanOperation.getPlanOperation(), fieldsPanel, fields,Condition.CORRELATION_EXISTS,null);
				
				
				fieldsPanel.revalidate();
				fieldsPanel.repaint();

			}
		});
		
		this.add(columnLabel, BorderLayout.NORTH);
		
		this.add(scrollPane, BorderLayout.CENTER);

		JPanel jpanel = new JPanel();
		jpanel.setLayout(new GridBagLayout());
		jpanel.setBackground(Color.white);
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.insets = new Insets(4, 4, 4, 4);
		gbc.anchor = GridBagConstraints.WEST;
		gbc.fill = GridBagConstraints.HORIZONTAL;
		jpanel.add(new JLabel("For Each Tuple of: "), gbc);
		gbc.gridx = 0;
		gbc.gridy = 1;
		jpanel.add(operations, gbc);
		
		
		gbc.gridx = 0;
		gbc.gridy = 2;
		jpanel.add(columns, gbc);
		gbc.gridx = 0;
		gbc.gridy = 3;
		jpanel.add(value, gbc);
		gbc.gridx = 0;
		gbc.gridy = 4;
		jpanel.add(correlation, gbc);
		
		
		gbc.gridx = 0;
		gbc.gridy = 5;
		jpanel.add(applyButton, gbc);
		gbc.gridx = 0;
		gbc.gridy = 6;
		jpanel.add(execute, gbc);
		gbc.gridx = 0;
		gbc.gridy = 7;
		gbc.fill = GridBagConstraints.BOTH;
		jpanel.add(removeOPButton, gbc);
		this.add(jpanel,BorderLayout.SOUTH);
	}

	
	
	public void execute() {
		if(drawPlanOperation!=null){
		
			Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
				
				public void run(ITransaction transaction) {
					//drawPlanOperation.getPlanOperation().getPlan().setTransaction(transaction);
					
					
					FilterOperation filter = (FilterOperation) drawPlanOperation.getPlanOperation().copy(new Plan(transaction), null);	

					ITable tableResult = filter.execute();
					DrawTable ij = new DrawTable(transaction, tableResult);
					
					ij.reloadMatriz();
					transaction.commit();
				}

				@Override
				public void onFail(ITransaction transaction, Exception e) {
					transaction.abort();
					
				}
				
			});
			
		}
		
	}
	

	public void apply() {
		
		if(drawPlanOperation!=null){
		
			FilterOperation filter = ((FilterOperation)drawPlanOperation.getPlanOperation());
			
			AbstractPlanOperation op = (AbstractPlanOperation) operations.getSelectedItem();
			
			if(op!=null){
				List<AbstractPlanOperation> list = filter.getOperationsResults();
				for (int i = 0; i < list.size(); i++) {
					if(op == list.get(i)) {
						filter.setTargetPosition(i);
					}
				}
			}
			filter.getAttributesOperatorsValues().clear();
			for (FilterField filterField : fields) {
				Condition c = filterField.getAov().copy();
				if(c.getTable2()!=null) {
					
					List<AbstractPlanOperation> list = filter.getOperationsResults();
					for (int i = 0; i < list.size(); i++) {
						if(c.getTable2().equals(list.get(i).toString())) {
							c.setTable2(String.valueOf(i));
							break;
						}
					}
					
				}
			
				filter.getAttributesOperatorsValues().add(c);
			}
		}
		
	}
	
}
