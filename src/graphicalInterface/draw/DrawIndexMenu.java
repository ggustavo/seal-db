package graphicalInterface.draw;

import javax.swing.JPanel;
import java.awt.GridBagLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

import java.awt.GridBagConstraints;
import javax.swing.JComboBox;
import java.awt.Insets;
import javax.swing.JTextField;
import javax.swing.SwingConstants;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.Column;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.transactionManager.ITransaction;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import java.awt.Color;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.Vector;
import java.awt.event.ActionEvent;

public class DrawIndexMenu extends JPanel {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private JTextField order;

	/**
	 * Create the panel.
	 */
	
	JComboBox<String> table;
	JComboBox<String> column;
	
	public DrawIndexMenu(DrawIndex drawIndex) {
		setBackground(Color.WHITE);
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0};
		gridBagLayout.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gridBagLayout.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 1.0, 0.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		setLayout(gridBagLayout);
		
		JLabel label_1 = new JLabel("    ");
		GridBagConstraints gbc_label_1 = new GridBagConstraints();
		gbc_label_1.insets = new Insets(0, 0, 5, 5);
		gbc_label_1.gridx = 1;
		gbc_label_1.gridy = 1;
		add(label_1, gbc_label_1);
		
		JLabel lblSchema = new JLabel("   Schema:");
		GridBagConstraints gbc_lblSchema = new GridBagConstraints();
		gbc_lblSchema.anchor = GridBagConstraints.WEST;
		gbc_lblSchema.insets = new Insets(0, 0, 5, 5);
		gbc_lblSchema.gridx = 1;
		gbc_lblSchema.gridy = 2;
		add(lblSchema, gbc_lblSchema);
		
		JComboBox<String> schema = new JComboBox<String>((Vector<String>)Kernel.getSchemasNames());
		schema.setMaximumRowCount(12);
		schema.addMouseListener(new MouseListener() {
			int lastSize = 0;
			public void mouseReleased(MouseEvent e) {}	
			public void mousePressed(MouseEvent e) {}
			public void mouseExited(MouseEvent e) {}
			public void mouseEntered(MouseEvent e) {
				
			}
			
			public void mouseClicked(MouseEvent e) {
				Vector<String> schemas = (Vector<String>)Kernel.getSchemasNames();
			

				if(lastSize != 0 && lastSize != schemas.size()){
					schema.setModel(new DefaultComboBoxModel<>(schemas));					
				}
			
				lastSize = schemas.size();
				
				ISchema dataSchema = Kernel.getCatalog().getSchemabyName(schema.getSelectedItem().toString());
				if(dataSchema!=null && dataSchema.getTables() != null && !dataSchema.getTables().isEmpty()) {
					table.removeAllItems();		
					table.revalidate();
					table.repaint();
					column.removeAllItems();
					column.revalidate();
					column.repaint();
				}
			}
		});
		
		
		GridBagConstraints gbc_schema = new GridBagConstraints();
		gbc_schema.insets = new Insets(0, 0, 5, 5);
		gbc_schema.fill = GridBagConstraints.HORIZONTAL;
		gbc_schema.gridx = 4;
		gbc_schema.gridy = 2;
		add(schema, gbc_schema);
		
		JLabel lblTable = new JLabel("   Table:");
		GridBagConstraints gbc_lblTable = new GridBagConstraints();
		gbc_lblTable.anchor = GridBagConstraints.WEST;
		gbc_lblTable.insets = new Insets(0, 0, 5, 5);
		gbc_lblTable.gridx = 1;
		gbc_lblTable.gridy = 3;
		add(lblTable, gbc_lblTable);
		
		table = new JComboBox<String>();
		table.addMouseListener(new MouseListener() {
			int lastSchema = -1;
			public void mouseReleased(MouseEvent e) {}	
			public void mousePressed(MouseEvent e) {}
			public void mouseExited(MouseEvent e) {}
			public void mouseEntered(MouseEvent e) {
				ISchema dataSchema = Kernel.getCatalog().getSchemabyName(schema.getSelectedItem().toString());
				if(dataSchema!=null && dataSchema.getTables() != null && !dataSchema.getTables().isEmpty()) {
					if(dataSchema.getId()==lastSchema) {
						return;
					}
					lastSchema = dataSchema.getId();
					table.removeAllItems();
					for (ITable t : dataSchema.getTables()) {
						if(!t.isTemp())table.addItem(t.getName());
					}
					table.revalidate();
					table.repaint();
					column.removeAllItems();
					column.revalidate();
					column.repaint();
				}else {
					table.removeAllItems();
					table.revalidate();
					table.repaint();
					column.removeAllItems();
					column.revalidate();
					column.repaint();
				}
				
			}
			
			public void mouseClicked(MouseEvent e) {	
				
			}
		});
		
		
		
		GridBagConstraints gbc_table = new GridBagConstraints();
		gbc_table.insets = new Insets(0, 0, 5, 5);
		gbc_table.fill = GridBagConstraints.HORIZONTAL;
		gbc_table.gridx = 4;
		gbc_table.gridy = 3;
		add(table, gbc_table);
		
		JLabel lblColumn = new JLabel("   Column:");
		GridBagConstraints gbc_lblColumn = new GridBagConstraints();
		gbc_lblColumn.anchor = GridBagConstraints.WEST;
		gbc_lblColumn.insets = new Insets(0, 0, 5, 5);
		gbc_lblColumn.gridx = 1;
		gbc_lblColumn.gridy = 4;
		add(lblColumn, gbc_lblColumn);
		
		column = new JComboBox<String>();
		column.addMouseListener(new MouseListener() {
			int lastTable= -1;
			public void mouseReleased(MouseEvent e) {}	
			public void mousePressed(MouseEvent e) {}
			public void mouseExited(MouseEvent e) {}
			public void mouseEntered(MouseEvent e) {
				ISchema dataSchema = Kernel.getCatalog().getSchemabyName(schema.getSelectedItem().toString());
				if(dataSchema!=null && dataSchema.getTables() != null && !dataSchema.getTables().isEmpty()) {
					if(table.getSelectedItem() == null) {
						column.removeAllItems();
						column.revalidate();
						column.repaint();	
						return;
					}
					ITable tbl = dataSchema.getTableByName(table.getSelectedItem().toString());
					
					if(tbl != null) {
						if(lastTable == tbl.getTableID())return;
						column.removeAllItems();
						column.revalidate();
						column.repaint();	
						lastTable = tbl.getTableID();
						for (Column c : tbl.getColumns()) {
							column.addItem(c.getName());
						}
						
					}else {
						column.removeAllItems();
						column.revalidate();
						column.repaint();
					}
					
					
				}else {
					
					column.removeAllItems();
					column.revalidate();
					column.repaint();
				}
				
			}
			
			public void mouseClicked(MouseEvent e) {	
				
			}
		});
		
		
		GridBagConstraints gbc_column = new GridBagConstraints();
		gbc_column.insets = new Insets(0, 0, 5, 5);
		gbc_column.fill = GridBagConstraints.HORIZONTAL;
		gbc_column.gridx = 4;
		gbc_column.gridy = 4;
		add(column, gbc_column);
		
		JLabel lblOrder = new JLabel("   Order:");
		GridBagConstraints gbc_lblOrder = new GridBagConstraints();
		gbc_lblOrder.anchor = GridBagConstraints.WEST;
		gbc_lblOrder.insets = new Insets(0, 0, 5, 5);
		gbc_lblOrder.gridx = 1;
		gbc_lblOrder.gridy = 6;
		add(lblOrder, gbc_lblOrder);
		
		order = new JTextField();
		order.setText("4");
		order.setHorizontalAlignment(SwingConstants.TRAILING);
		GridBagConstraints gbc_order = new GridBagConstraints();
		gbc_order.fill = GridBagConstraints.HORIZONTAL;
		gbc_order.insets = new Insets(0, 0, 5, 5);
		gbc_order.gridx = 4;
		gbc_order.gridy = 6;
		add(order, gbc_order);
		order.setColumns(10);
		
		JLabel label = new JLabel("    ");
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.insets = new Insets(0, 0, 5, 5);
		gbc_label.gridx = 4;
		gbc_label.gridy = 8;
		add(label, gbc_label);
		
		JButton btnCreate = new JButton("Create");
		btnCreate.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				try {
					if(table.getSelectedItem() == null || schema.getSelectedItem() == null || column.getSelectedItem() == null) {
						
						return;
					}
					int o = Integer.parseInt(order.getText());					
					if(o <= 2) {
						throw new IllegalArgumentException("Illegal branching factor: "+ o);
					}
					ISchema s = Kernel.getCatalog().getSchemabyName(schema.getSelectedItem().toString());
					if(s==null) {
						
						return;
					}
					ITable tbl = s.getTableByName(table.getSelectedItem().toString());
					if(tbl==null) {
						
						return;
					}
					
					DBConnection tempConnection = Kernel.getTransactionManager().getConnectionService().getSystemConnection(schema.getSelectedItem().toString());
					ITransaction transaction = Kernel.getExecuteTransactions().begin(tempConnection, false, false);
					
					if(tbl.getIdColumn(column.getSelectedItem().toString()) >= 0) {
						
						drawIndex.load(transaction, tbl, tbl.getColumns()[tbl.getIdColumn(column.getSelectedItem().toString())], o);						
					}else {
						
					}
				}catch (Exception e) {
					JOptionPane.showMessageDialog(null, e.getMessage());
					Kernel.exception(DrawIndex.class, e);
				}
				
				
			}
		});
		GridBagConstraints gbc_btnCreate = new GridBagConstraints();
		gbc_btnCreate.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnCreate.insets = new Insets(0, 0, 5, 5);
		gbc_btnCreate.gridx = 4;
		gbc_btnCreate.gridy = 9;
		add(btnCreate, gbc_btnCreate);

	}
	

}
