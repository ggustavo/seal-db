package graphicalInterface.menus.subMenus;


import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.ITable;
import graphicalInterface.draw.DrawPlanOperation;


public class TableMenu extends AbstractSubMenu{

	private static final long serialVersionUID = 1L;
	
	private JComboBox<ITable> tables;
	private DrawPlanOperation drawPlanOperation; 

	
	public void update(DrawPlanOperation drawPlanOperation){
		this.drawPlanOperation = drawPlanOperation;
		ITable table = null;
		tables.removeAllItems();
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		if(drawPlanOperation!=null){
			table = drawPlanOperation.getPlanOperation().getResultLeft();
			if(table!=null){
				tables.addItem(table);
				tables.setSelectedItem(table);
			}
		}

		
		
		List<ITable> tablesList = Kernel.getCatalog().getSchemabyName(connection.getSchemaName()).getTables();
			for (ITable iTable : tablesList) {
				if(!iTable.isTemp() && iTable != table ) {
					tables.addItem(iTable);				
				}
			}	
		
		
	}
	
	
	
	public TableMenu(DBConnection connection) {
		super(connection);
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		this.setBackground(Color.white);
		this.setLayout(new FlowLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		tables = new JComboBox<ITable>();
		JButton applyButton = new JButton("Apply");
		applyButton.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				apply();
			}
		});

		JButton removeOPButton = new JButton("Remove Operation");
		removeOPButton.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				removeFromPlan(drawPlanOperation);

			}
		});

		JPanel jpanel = new JPanel();
		jpanel.setLayout(new GridBagLayout());
		jpanel.setBackground(Color.white);
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.insets = new Insets(4, 4, 4, 4);
		gbc.anchor = GridBagConstraints.WEST;
		gbc.fill = GridBagConstraints.HORIZONTAL;
		jpanel.add(new JLabel("Choice a Table: "), gbc);
		gbc.gridx = 0;
		gbc.gridy = 1;
		jpanel.add(tables, gbc);
		gbc.gridx = 0;
		gbc.gridy = 2;
		jpanel.add(applyButton, gbc);
		gbc.gridx = 0;
		gbc.gridy = 3;
		gbc.fill = GridBagConstraints.BOTH;
		jpanel.add(removeOPButton, gbc);
		this.add(jpanel);
	}




	
	public void apply() {
		ITable table= (ITable) tables.getSelectedItem();
		if(drawPlanOperation!=null && table!=null){
		
			drawPlanOperation.getPlanOperation().setResultLeft(table);
			drawPlanOperation.getLabelName().setText(drawPlanOperation.getPlanOperation().getName());
			
		}
		
	}
	
}
