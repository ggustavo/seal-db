package graphicalInterface.menus;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JTextField;

import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.FilterOperation;
import graphicalInterface.images.ImagensController;
import graphicalInterface.util.MeshLayout;


public class FilterField extends JPanel{
	

	private static final long serialVersionUID = 1L;
	
	public JComboBox<AbstractPlanOperation> results = new JComboBox<AbstractPlanOperation>();
	public JComboBox<String> attributesComboBox1 = new JComboBox<String>();
	public JComboBox<String> attributesComboBox2 = new JComboBox<String>();

	public JComboBox<String> operator2ComboBox = new JComboBox<String>(new String[]{"==","!=",">","<",">=","<=","in","not in"});
	public JComboBox<String> operatorComboBox = new JComboBox<String>(new String[]{"==","!=",">","<",">=","<="});
	public JComboBox<String> externalComboBox = new JComboBox<String>(new String[]{"and","or"});
	public JComboBox<String> exists = new JComboBox<String>(new String[]{"Exists","Not Exists"});
	
	public JTextField value = new JTextField();
	
	public JButton deleteButton = new JButton();
	
	
	
	int type;
	
	
	//render
	public void update(AbstractPlanOperation operation, Condition aov) {
		
		FilterOperation filter = (FilterOperation)operation;
		
		if(filter==null || aov == null || filter.getForeache() == null) {
			
			attributesComboBox1.removeAllItems();
			attributesComboBox2.removeAllItems();
			results.removeAllItems();
			operatorComboBox.removeAllItems();
			operator2ComboBox.removeAllItems();
			exists.removeAllItems();
			externalComboBox.removeAllItems();
			value.setText("");
			return;
		}
		
		
		if(type == Condition.CORRELATION_EXISTS || 
			type == Condition.CORRELATION_NOT_EXISTS) {
			
			if(aov.getType() == Condition.CORRELATION_NOT_EXISTS) {
				setSelected(exists, "Not Exists");
			}else {
				setSelected(exists, "Exists");
				
			}
			
			addItems(attributesComboBox1, filter.getForeache().getPossiblesColumnNames());
			attributesComboBox1.addItem(FilterOperation.NONE);
			
		
			
			if(aov.getAtribute()!=null)setSelected(attributesComboBox1, aov.getAtribute());
			if(aov.getOperator()!=null)setSelected(operator2ComboBox, aov.getOperator());
			
			if(attributesComboBox1.getSelectedItem()!=null && attributesComboBox1.getSelectedItem().toString().equals(FilterOperation.NONE)) {
				operator2ComboBox.setEnabled(false);
			}else {
				operator2ComboBox.setEnabled(true);
			}
			
			List<AbstractPlanOperation> operationsList = filter.getOperationsResults();
			if (operationsList != null && !operationsList.isEmpty()) {
				operationsList.remove(filter.getForeache());
				
				addItems(results, operationsList);
				if(aov.getTable2()!=null)results.setSelectedItem(aov.getTable2());

				AbstractPlanOperation selected = (AbstractPlanOperation) results.getSelectedItem();

				if (selected != null) {
					addItems(attributesComboBox2, selected.getPossiblesColumnNames());
					if(aov.getValue()!=null)attributesComboBox2.setSelectedItem(aov.getValue());
				}
			}else {
				results.removeAllItems();
				aov.setTable2(null);
				attributesComboBox2.removeAllItems();
				aov.setValue(null);
			}
		}
		

		if(type == Condition.COLUMN_COLUMN) {
			addItems(attributesComboBox1, filter.getForeache().getPossiblesColumnNames());
			if(aov.getAtribute()!=null)setSelected(attributesComboBox1, aov.getAtribute());
			
			if(aov.getOperator()!=null)setSelected(operatorComboBox, aov.getOperator());
			
			addItems(attributesComboBox2, filter.getForeache().getPossiblesColumnNames());
			if(aov.getValue()!=null)setSelected(attributesComboBox2, aov.getValue());
		}
		
		if(type == Condition.COLUMN_VALUE) {
			addItems(attributesComboBox1, filter.getForeache().getPossiblesColumnNames());
			if(aov.getAtribute()!=null)setSelected(attributesComboBox1, aov.getAtribute());
			
			if(aov.getOperator()!=null)setSelected(operatorComboBox, aov.getOperator());
			
			if(aov.getValue()!=null)value.setText(aov.getValue());

		}
		
		setSelected(externalComboBox, aov.getExternalOperator());

		this.revalidate();
		this.repaint();
	}
	
	
	
	public Condition getAov(){
		Condition aov = new Condition();
		
		if(type == Condition.CORRELATION_EXISTS || 
			type == Condition.CORRELATION_NOT_EXISTS) {
			
			if(exists.getSelectedItem()!=null)
				if(exists.getSelectedItem().toString().equals("Not Exists")) {
					aov.setType(Condition.CORRELATION_NOT_EXISTS);
					type = Condition.CORRELATION_NOT_EXISTS;
				}else {
					aov.setType(Condition.CORRELATION_EXISTS);
					type = Condition.CORRELATION_EXISTS;
				}
			
			if(attributesComboBox1.getSelectedItem()!=null)aov.setAtribute((String)attributesComboBox1.getSelectedItem());
			if(operator2ComboBox.getSelectedItem()!=null)aov.setOperator((String) operator2ComboBox.getSelectedItem());
			AbstractPlanOperation selected = (AbstractPlanOperation) results.getSelectedItem();

			if (selected != null) {	
				aov.setTable2(selected.toString());
				if(attributesComboBox2.getSelectedItem()!=null)aov.setValue((String) attributesComboBox2.getSelectedItem());
			}
			
			
		}
		
		if(type == Condition.COLUMN_COLUMN) {
			aov.setType(Condition.COLUMN_COLUMN);
			if(attributesComboBox1.getSelectedItem()!=null) aov.setAtribute((String)attributesComboBox1.getSelectedItem());
			if(operatorComboBox.getSelectedItem()!=null) aov.setOperator((String) operatorComboBox.getSelectedItem());
			if(attributesComboBox2.getSelectedItem()!=null) aov.setValue((String)attributesComboBox2.getSelectedItem());
		}
		
		if(type == Condition.COLUMN_VALUE) {
			aov.setType(Condition.COLUMN_VALUE);
			if(attributesComboBox1.getSelectedItem()!=null) aov.setAtribute((String)attributesComboBox1.getSelectedItem());
			if(operatorComboBox.getSelectedItem()!=null) aov.setOperator((String) operatorComboBox.getSelectedItem());
			if(aov.getValue()!=null)value.setText(aov.getValue());
			if(value.getText()!=null)aov.setValue(value.getText());
		}
		
		//AND | OR
		if(externalComboBox.getSelectedItem()!=null)aov.setExternalOperator((String) externalComboBox.getSelectedItem());
		return aov;
	}

	
	public void addItems(JComboBox<String> comobox,String[] attributesArray) {

		if (attributesArray != null) {
			comobox.removeAllItems();

			for (int i = 0; i < attributesArray.length; i++) {
				comobox.addItem(attributesArray[i]);
			}

			comobox.revalidate();
			comobox.repaint();
		}
	}
	
	public void addItems(JComboBox<AbstractPlanOperation> comobox,List<AbstractPlanOperation> attributesArray) {

		if (attributesArray != null) {
			comobox.removeAllItems();
			
			for (AbstractPlanOperation abs : attributesArray) {
				comobox.addItem(abs);
				
			}

			comobox.revalidate();
			comobox.repaint();
		}
	}
	
	

	
	
	public FilterField(AbstractPlanOperation operation, JPanel panel, List<FilterField> fields, int type, Condition aov){
		
		this.setBackground(Color.white);
		//this.setLayout(new BoxLayout(this, BoxLayout.LINE_AXIS));
		this.type = type;
		
		if(aov==null) {
			aov = new Condition();
			aov.setType(type);
		}
		
		deleteButton.setBorder(null);
		deleteButton.setPreferredSize(new Dimension(20, 20));
		deleteButton.setIcon(ImagensController.CLOSE);
		deleteButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				fields.remove(FilterField.this);
				panel.remove(FilterField.this);
				panel.revalidate();
				panel.repaint();
			}
		});
//		updateOnClick(operation,aov,attributesComboBox1);
//		updateOnClick(operation,aov,attributesComboBox2);
//		updateOnClick(operation,aov,results);
//		updateOnClick(operation,aov,operatorComboBox);
//		updateOnClick(operation,aov,operator2ComboBox);
//		updateOnClick(operation,aov,exists);
//		updateOnClick(operation,aov,externalComboBox);
		
		attributesComboBox1.setBorder(null);
		attributesComboBox2.setBorder(null);
		results.setBorder(null);
		operatorComboBox.setBorder(null);
		operator2ComboBox.setBorder(null);
		exists.setBorder(null);
		externalComboBox.setBorder(null);
		
		if(type == Condition.COLUMN_COLUMN) {
			this.setLayout(new MeshLayout(1, 5));
			this.add(attributesComboBox1);
			this.add(operatorComboBox);
			this.add(attributesComboBox2);
		}else
		
		if(type == Condition.COLUMN_VALUE) {
			this.setLayout(new MeshLayout(1, 5));
			this.add(attributesComboBox1);
			this.add(operatorComboBox);
			value.setPreferredSize(new Dimension(100,30));
			this.add(value);
		}else
		
		if(type == Condition.CORRELATION_EXISTS || 
				type == Condition.CORRELATION_NOT_EXISTS) {
			this.setLayout(new MeshLayout(1, 7));
			this.add(exists);
			this.add(attributesComboBox1);
			this.add(operator2ComboBox);
			this.add(results);
			this.add(attributesComboBox2);
			
			attributesComboBox1.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					
					if(attributesComboBox1.getSelectedItem()!=null && attributesComboBox1.getSelectedItem().toString().equals(FilterOperation.NONE)) {
						operator2ComboBox.setEnabled(false);
					}else {
						operator2ComboBox.setEnabled(true);
					}
					
				}
			});
			
		}else {
			return;
		}

		this.add(externalComboBox);
		this.add(deleteButton);
		fields.add(this);
		panel.add(this);
		update(operation,aov);

	}
	
	
	public void updateOnClick(AbstractPlanOperation operation, Condition aov, JComboBox<?> comboBox){
		for (int i = 0; i < comboBox.getComponentCount(); i++) {
			Component component = comboBox.getComponent(i);
			if (component instanceof AbstractButton) {
			
				component.addMouseListener(new MouseListener() {
		    		
					public void mouseReleased(MouseEvent e) {}
					public void mousePressed(MouseEvent e) {}
					public void mouseExited(MouseEvent e) {}
					public void mouseEntered(MouseEvent e) {}
					
					public void mouseClicked(MouseEvent e) {
						
					update(operation,aov);
					
					}
				});
			
			}
		}
	}
	
	private void setSelected(JComboBox<?> comboBox, Object value){
		Object item;
		if (value != null)
			for (int i = 0; i < comboBox.getItemCount(); i++) {
				item = comboBox.getItemAt(i);
				if (item == (value) || item.equals(value) || item.toString().equals(value)) {
					comboBox.setSelectedIndex(i);
					return;
				}
			}

    }

	
}