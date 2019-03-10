package graphicalInterface.menus;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;

import javax.swing.AbstractButton;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JPanel;


import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.AggregationOperation;
import graphicalInterface.images.ImagensController;
import graphicalInterface.util.MeshLayout;


public class AggregationField extends JPanel{
	
	

	private static final long serialVersionUID = 1L;
	
	public JComboBox<String> attributesComboBox = new JComboBox<String>();
	public JComboBox<String> operatorComboBox = new JComboBox<String>(new String[]{
			
			AggregationOperation.GROUPING,	
			AggregationOperation.COUNT,
			AggregationOperation.SUM,
			AggregationOperation.MAX,
			AggregationOperation.MIN,
			AggregationOperation.AVG,
		//	AggregationOperation.STDEV,
		//	AggregationOperation.VAR 
	});
	public JButton deleteButton = new JButton();
	//public JButton deleteButton = new JButton(UIManager.getIcon("InternalFrame.closeIcon"));
	public JPanel this_panel = this;
	
	public void update(AbstractPlanOperation operation){
		String[] attributesArray = operation.getPossiblesColumnNames();
		
		if(attributesArray != null){	
			attributesComboBox.removeAllItems();
			if(attributesArray!=null){
				
				for (int i = 0; i < attributesArray.length; i++) {
					attributesComboBox.addItem(attributesArray[i]);
				}
			}
			
			attributesComboBox.revalidate();
			attributesComboBox.repaint();
		}
	}
	
	public void update(AbstractPlanOperation operation, Condition aov){
		update(operation);
		setSelected(attributesComboBox, aov.getAtribute());
		setSelected(operatorComboBox, aov.getOperator());
		this.revalidate();
		this.repaint();
	}

	
	public JComboBox<String> getAttributesComboBox() {
		return attributesComboBox;
	}




	public JComboBox<String> getOperatorComboBox() {
		return operatorComboBox;
	}


	public AggregationField(AbstractPlanOperation operation, JPanel panel, ArrayList<AggregationField> fields){
		fields.add(this);
		this.setBackground(Color.white);
		//this.setLayout(new BoxLayout(this, BoxLayout.LINE_AXIS));
		this.setLayout(new MeshLayout(1, 4));
		updateOnClick(operation);
		update(operation);
		deleteButton.setBorder(null);
		deleteButton.setPreferredSize(new Dimension(20, 20));
		deleteButton.setIcon(ImagensController.CLOSE);
		deleteButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				fields.remove(this_panel);
				panel.remove(this_panel);
				panel.revalidate();
				panel.repaint();
			}
		});
		attributesComboBox.setBorder(null);
		operatorComboBox.setBorder(null);
		this.add(attributesComboBox);
		this.add(operatorComboBox);
		this.add(deleteButton);
	
	}
	
	
	public void updateOnClick(AbstractPlanOperation operation){
		for (int i = 0; i < attributesComboBox.getComponentCount(); i++) {
			Component component = attributesComboBox.getComponent(i);
			if (component instanceof AbstractButton) {
			
				component.addMouseListener(new MouseListener() {
		    		
					public void mouseReleased(MouseEvent e) {}
					public void mousePressed(MouseEvent e) {}
					public void mouseExited(MouseEvent e) {}
					public void mouseEntered(MouseEvent e) {}
					
					public void mouseClicked(MouseEvent e) {
						
					update(operation);
					
					}
				});
			
			}
		}
	}
	
	private void setSelected(JComboBox<?> comboBox, Object value)
    {
        Object item;
        for (int i = 0; i < comboBox.getItemCount(); i++)
        {
            item = comboBox.getItemAt(i);
            if (item == (value) || item.equals(value))
            {
                comboBox.setSelectedIndex(i);
               return;
            }
        }
        
 
    }
	
	
	
}