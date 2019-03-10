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
import java.util.logging.Level;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.SwingConstants;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SortOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.draw.DrawTable;


public class SortMenu extends AbstractSubMenu{

	private static final long serialVersionUID = 1L;
	private ArrayList<JRadioButton> fields;
	private DrawPlanOperation drawPlanOperation;
	private JPanel fieldsPanel;
	private JComboBox<String> jComboBoxOrder;
	
	public void update(DrawPlanOperation drawPlanOperation){
		this.drawPlanOperation = drawPlanOperation;
		fields.clear();
		fieldsPanel.removeAll();
		if(drawPlanOperation!=null && drawPlanOperation.getPlanOperation().getLeft()!=null){
			
			String[] colunms = drawPlanOperation.getPlanOperation().getPossiblesColumnNames();
		//	ButtonGroup buttonGroup = new ButtonGroup();
			
			for (int i = 0; colunms!= null && i < colunms.length ; i++) {
				JRadioButton rdb = new JRadioButton(colunms[i],false);
				rdb.setBackground(Color.white);
		//		buttonGroup.add(rdb);
				fields.add(rdb);
				fieldsPanel.add(rdb);
			}
			
			if(((SortOperation) drawPlanOperation.getPlanOperation()).getColumnSorted() != null){
				
				//String projectedColunms[] =((ProjectionOperation) drawPlanOperation.getPlanOperation()).getAttributesProjected();
				List<String> colunmsName = ((SortOperation) drawPlanOperation.getPlanOperation()).getColumnSorted();
				for (String colunmName : colunmsName) {
					for (JRadioButton rdn : fields) {
						if(rdn.getText().equals(colunmName)){
							rdn.setSelected(true);
							break;
						}
					}
				}
			}else{
				//if(!fields.isEmpty())fields.get(0).setSelected(true);
			}
			setSelected(jComboBoxOrder,((SortOperation) drawPlanOperation.getPlanOperation()).isOrder()?"Crescent":"Decrescent");
			
			
			
			fieldsPanel.revalidate();
			fieldsPanel.repaint();
		}
	}
	
	
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
	
	public SortMenu(DBConnection connection) {
		super(connection);
		jComboBoxOrder = new JComboBox<String>(new String[]{"Crescent","Decrescent"});
		((JLabel)jComboBoxOrder.getRenderer()).setHorizontalAlignment(SwingConstants.CENTER);
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		
		this.setBackground(Color.white);
		this.setLayout(new BorderLayout());
		
		fieldsPanel = new JPanel();
		fieldsPanel.setLayout(new GridLayout(50, 1));
		fieldsPanel.setBackground(Color.WHITE);
		fieldsPanel.setBorder(BorderFactory.createLineBorder(Color.black));
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.getViewport().add(fieldsPanel);
		
		fields = new ArrayList<JRadioButton>();
		
		JLabel columnLabel = new JLabel("  Sort Operation Options  ");//Column: 

		JButton executeButton = new JButton("Execute");
		executeButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				if(drawPlanOperation!=null){
					
					Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
						
						public void run(ITransaction transaction) {
							//drawPlanOperation.getPlanOperation().getPlan().setTransaction(transaction);
							
							SortOperation sortOP = (SortOperation) drawPlanOperation.getPlanOperation().copy(new Plan(transaction), null);
							
							ArrayList<String> columnsArray = new ArrayList<>();				
							for (JRadioButton jRadioButton : fields) {
								if(jRadioButton.isSelected()){
									columnsArray.add(jRadioButton.getText());
								}
							}
							//if(!columnsArray.isEmpty()){
						//		projecOP.setAttributesProjected(columnsArray.toArray(new String[columnsArray.size()]));
						//	}
							if(columnsArray.isEmpty()){
								Kernel.log(this.getClass(),"columns empty",Level.WARNING);
							}
							sortOP.setColumnSorted(columnsArray);
							
							if(jComboBoxOrder.getSelectedItem().equals("Crescent")){
								sortOP.setOrder(true);
							}else{
								sortOP.setOrder(false);
							}
							
							
							ITable tableResult = sortOP.execute();
	
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
		jpanel.add(jComboBoxOrder, gbc);
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
		if(drawPlanOperation!=null){			
			SortOperation sortOP = (SortOperation) drawPlanOperation.getPlanOperation();
			
			ArrayList<String> columnsArray = new ArrayList<>();				
			for (JRadioButton jRadioButton : fields) {
				
				if(jRadioButton.isSelected()){
					columnsArray.add(jRadioButton.getText());
				}
	
			
			}

			if(!columnsArray.isEmpty())sortOP.setColumnSorted(columnsArray);
			
			if(jComboBoxOrder.getSelectedItem().equals("Crescent")){
				sortOP.setOrder(true);
			}else{
				sortOP.setOrder(false);
			}
		
	}

	}
	
	
	
	
}
