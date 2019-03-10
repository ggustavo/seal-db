package graphicalInterface.menus.subMenus;


import java.awt.BorderLayout;
import java.awt.Color;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;

import javax.swing.JButton;


import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.draw.DrawTable;
import graphicalInterface.menus.SelectionField;


public class SelectionMenu extends AbstractSubMenu{

	private static final long serialVersionUID = 1L;
	private ArrayList<SelectionField> fields;
	private DrawPlanOperation drawPlanOperation;
	private JPanel fieldsPanel;

	
	public void update(DrawPlanOperation drawPlanOperation){
		this.drawPlanOperation = drawPlanOperation;
		fields.clear();
		fieldsPanel.removeAll();
		if(drawPlanOperation!=null && drawPlanOperation.getPlanOperation().getLeft()!=null){
			List<Condition> aov = drawPlanOperation.getPlanOperation().getAttributesOperatorsValues();
			for (Condition aovi : aov) {
				SelectionField a = new SelectionField(drawPlanOperation.getPlanOperation().getLeft(), fieldsPanel, fields);
				a.update(drawPlanOperation.getPlanOperation().getLeft(),aovi);
				fieldsPanel.add(a);
			}
			fieldsPanel.revalidate();
			fieldsPanel.repaint();
		}
	}
	
	
	public SelectionMenu(DBConnection connection) {
		super(connection);
		
		
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		
		this.setBackground(Color.white);
		this.setLayout(new BorderLayout());
		
		fieldsPanel = new JPanel();
		fieldsPanel.setLayout(new GridLayout(50, 1));
		fieldsPanel.setBackground(Color.WHITE);
		fieldsPanel.setBorder(BorderFactory.createLineBorder(Color.black));
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.getViewport().add(fieldsPanel);
		
		fields = new ArrayList<SelectionField>();
		
		JLabel columnLabel = new JLabel("  Selection Operation Options  ");//Column: 

		JButton executeButton = new JButton("Execute");
		executeButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				if(drawPlanOperation!=null){
					
					Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
						
						public void run(ITransaction transaction) {
							
							//drawPlanOperation.getPlanOperation().getPlan().setTransaction(transaction);
							
							SelectionOperation selectOP = (SelectionOperation) drawPlanOperation.getPlanOperation().copy(new Plan(transaction), null);
							
							selectOP.getAttributesOperatorsValues().clear();
							for (SelectionField menuField : fields) {
								//LogError.save(this.getClass(),menuField.getAttributesComboBox().getSelectedItem().toString()+ " "+menuField.getOperatorComboBox().getSelectedItem().toString()+" "+ menuField.getValue().getText());
								selectOP.getAttributesOperatorsValues().add(new Condition(menuField.getAttributesComboBox().getSelectedItem().toString(), menuField.getOperatorComboBox().getSelectedItem().toString(), menuField.getValue().getText()));
							}
							
							ITable tableResult = selectOP.execute();
							//LogError.save(this.getClass(),selectOP.getName());
							
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
		});
		
		
		JButton addColunmButton = new JButton("Add");
		addColunmButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				
				if(drawPlanOperation.getPlanOperation().getLeft()!=null){
					SelectionField a = new SelectionField(drawPlanOperation.getPlanOperation().getLeft(), fieldsPanel, fields);
					
					fieldsPanel.add(a);
					fieldsPanel.revalidate();
					fieldsPanel.repaint();
				}
				
			}
		});
		
		JButton removeOP = new JButton("Remove Operation");
		removeOP.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				
				removeFromPlan(drawPlanOperation);
				
			}
		});
		JButton applyButton = new JButton("Apply");
		applyButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				apply();
			}
		});
		
		this.add(columnLabel, BorderLayout.NORTH);
		
		this.add(scrollPane, BorderLayout.CENTER);


		GridBagConstraints gbc = new GridBagConstraints();
		JPanel jpanel = new JPanel();
		jpanel.setLayout(new GridBagLayout());
		jpanel.setBackground(Color.white);
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.insets = new Insets(4, 4 , 4, 4);
		gbc.anchor = GridBagConstraints.WEST;
		gbc.fill = GridBagConstraints.HORIZONTAL;
		jpanel.add(addColunmButton, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 1;
		jpanel.add(applyButton, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 2;
		jpanel.add(executeButton, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 3;
		gbc.fill = GridBagConstraints.BOTH;
		jpanel.add(removeOP,gbc);
		
		this.add(jpanel, BorderLayout.SOUTH);
		
	}


	public void apply() {
		if(drawPlanOperation!=null && drawPlanOperation.getPlanOperation().getLeft()!=null ){	
		SelectionOperation selectOP = (SelectionOperation) drawPlanOperation.getPlanOperation();	
		selectOP.getAttributesOperatorsValues().clear();
		for (SelectionField menuField : fields) {
			//LogError.save(this.getClass(),menuField.getAttributesComboBox().getSelectedItem().toString()+ " "+menuField.getOperatorComboBox().getSelectedItem().toString()+" "+ menuField.getValue().getText());
			selectOP.getAttributesOperatorsValues().add(new Condition(menuField.getAttributesComboBox().getSelectedItem().toString(), menuField.getOperatorComboBox().getSelectedItem().toString(), menuField.getValue().getText()));
		}
		}
	}

	
	
	
	
}
