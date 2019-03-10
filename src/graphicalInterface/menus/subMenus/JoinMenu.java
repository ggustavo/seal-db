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
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawJoin;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.draw.DrawTable;
import graphicalInterface.menus.JoinField;



public class JoinMenu extends AbstractSubMenu{


	private static final long serialVersionUID = 1L;
	private ArrayList<JoinField> fields;
	private  JComboBox<String> joinAlgorithm;
	private DrawPlanOperation drawPlanOperation;
	private JPanel fieldsPanel;
	
	private void setSelected(JComboBox<?> comboBox, Object value) {
		Object item;
		for (int i = 0; i < comboBox.getItemCount(); i++) {
			item = comboBox.getItemAt(i);
			if (item == (value) || item.equals(value)) {
				comboBox.setSelectedIndex(i);
				return;
			}
		}

	}
	
	public void update(DrawPlanOperation drawPlanOperation){
		
		this.drawPlanOperation = drawPlanOperation;
		fields.clear();
		fieldsPanel.removeAll();
		if(drawPlanOperation!=null && drawPlanOperation.getPlanOperation().getLeft()!=null && drawPlanOperation.getPlanOperation().getRight()!=null){	
			
			if(((JoinOperation)drawPlanOperation.getPlanOperation()).getJoinAlgorithm()!=null){
				setSelected(joinAlgorithm, ((JoinOperation)drawPlanOperation.getPlanOperation()).getJoinAlgorithm().getClass().getSimpleName());
			}
			
			List<Condition> aov = drawPlanOperation.getPlanOperation().getAttributesOperatorsValues();
			for (Condition aovi : aov) {
				JoinField a = new JoinField(drawPlanOperation.getPlanOperation(), fieldsPanel, fields);
				a.update(drawPlanOperation.getPlanOperation(),aovi);
				fieldsPanel.add(a);
			}
			fieldsPanel.revalidate();
			fieldsPanel.repaint();
		}
	}

	public JoinMenu(DBConnection connection) {
		super(connection);
		this.joinAlgorithm = new JComboBox<String>((Vector<String>) Kernel.getJoinAlgorithmListNames());
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		
		
		
		this.setBackground(Color.white);
		this.setLayout(new BorderLayout());
		
		fieldsPanel = new JPanel();
		fieldsPanel.setLayout(new GridLayout(50, 1));
		fieldsPanel.setBackground(Color.WHITE);
		fieldsPanel.setBorder(BorderFactory.createLineBorder(Color.black));
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.getViewport().add(fieldsPanel);
		
		fields = new ArrayList<JoinField>();
		
		JLabel columnLabel = new JLabel("  Join Operation Options  ");

		JButton executeButton = new JButton("Execute");
		executeButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				if(drawPlanOperation!=null){
					Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
						
						public void run(ITransaction transaction) {
							//drawPlanOperation.getPlanOperation().getPlan().setTransaction(transaction);
							
							JoinOperation joinOP = (JoinOperation) drawPlanOperation.getPlanOperation().copy(new Plan(transaction), null);
							DrawJoin dj = new DrawJoin(joinOP);
							drawPlanOperation.getDrawPlan().getInitialFrame().addJoinLayer(dj);
							
							joinOP.setJoinAlgorithm(Kernel.getJoinAlgorithmIntance(joinAlgorithm.getSelectedItem().toString()));
							joinOP.getJoinAlgorithm().setiJoinAlgotithmListener(dj);
							
							joinOP.getAttributesOperatorsValues().clear();
							for (JoinField menuField : fields) {
								//LogError.save(this.getClass(),menuField.getAttributesComboBox().getSelectedItem().toString()+ " "+menuField.getOperatorComboBox().getSelectedItem().toString()+" "+ menuField.getValue().getText());
								joinOP.getAttributesOperatorsValues().add(new Condition(menuField.getAttributesComboBox1().getSelectedItem().toString(), menuField.getOperatorComboBox().getSelectedItem().toString(), menuField.getAttributesComboBox2().getSelectedItem().toString()));
							}
							
							ITable tableResult = joinOP.execute();
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
				
				if(drawPlanOperation.getPlanOperation().getLeft()!=null && drawPlanOperation.getPlanOperation().getRight()!=null){
					JoinField a = new JoinField(drawPlanOperation.getPlanOperation(), fieldsPanel, fields);
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
		gbc.insets = new Insets(4, 4 , 4, 4);
		gbc.anchor = GridBagConstraints.WEST;
		gbc.fill = GridBagConstraints.HORIZONTAL;
		
		gbc.gridx = 0;
		gbc.gridy = 0;
		jpanel.add(joinAlgorithm, gbc);
		gbc.gridx = 0;
		gbc.gridy = 1;
		jpanel.add(addColunmButton, gbc);
		gbc.gridx = 0;
		gbc.gridy = 2;
		jpanel.add(applyButton, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 3;
		jpanel.add(executeButton, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 4;
		gbc.fill = GridBagConstraints.BOTH;
		jpanel.add(removeOP,gbc);
		
		this.add(jpanel, BorderLayout.SOUTH);
		
	}

	
	public void apply() {
		if(drawPlanOperation!=null && drawPlanOperation.getPlanOperation().getRight()!=null && drawPlanOperation.getPlanOperation().getLeft()!=null){	
		JoinOperation joinOP = (JoinOperation) drawPlanOperation.getPlanOperation();
		joinOP.setJoinAlgorithm(Kernel.getJoinAlgorithmIntance(joinAlgorithm.getSelectedItem().toString()));
		joinOP.getAttributesOperatorsValues().clear();
		for (JoinField menuField : fields) {
			//LogError.save(this.getClass(),menuField.getAttributesComboBox().getSelectedItem().toString()+ " "+menuField.getOperatorComboBox().getSelectedItem().toString()+" "+ menuField.getValue().getText());
			joinOP.getAttributesOperatorsValues().add(new Condition(menuField.getAttributesComboBox1().getSelectedItem().toString(), menuField.getOperatorComboBox().getSelectedItem().toString(), menuField.getAttributesComboBox2().getSelectedItem().toString()));
		}
		}
	}

	

}
