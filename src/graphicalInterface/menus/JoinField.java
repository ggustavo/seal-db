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
import graphicalInterface.images.ImagensController;
import graphicalInterface.util.MeshLayout;


public class JoinField extends JPanel{
	

	private static final long serialVersionUID = 1L;
	
	public JComboBox<String> attributesComboBox1 = new JComboBox<String>();
	public JComboBox<String> operatorComboBox = new JComboBox<String>(new String[]{"==","!=",">","<",">=","<="});
	public JComboBox<String> attributesComboBox2 = new JComboBox<String>();
	//public JButton deleteButton = new JButton(UIManager.getIcon("InternalFrame.closeIcon"));
	public JButton deleteButton = new JButton();
	public JPanel this_panel = this;
	
	public void update(AbstractPlanOperation operation){
		String[]attributesArray = operation.getRight().getPossiblesColumnNames();
		 attributesArray = operation.getLeft().getPossiblesColumnNames();
		
		if(attributesArray != null){	
			attributesComboBox1.removeAllItems();
			if(attributesArray!=null){
				
				for (int i = 0; i < attributesArray.length; i++) {
					attributesComboBox1.addItem(attributesArray[i]);
				}
			}
			
			attributesComboBox1.revalidate();
			attributesComboBox1.repaint();
		}
		
		attributesArray = operation.getRight().getPossiblesColumnNames();
		
		if(attributesArray != null){	
			attributesComboBox2.removeAllItems();
			if(attributesArray!=null){
				
				for (int i = 0; i < attributesArray.length; i++) {
					attributesComboBox2.addItem(attributesArray[i]);
				}
			}
			
			attributesComboBox2.revalidate();
			attributesComboBox2.repaint();
		}
		
	}
	
	public void update(AbstractPlanOperation operation, Condition aov){
		update(operation);
		setSelected(attributesComboBox1, aov.getAtribute());
		setSelected(operatorComboBox, aov.getOperator());
		setSelected(attributesComboBox2, aov.getValue());
		this.revalidate();
		this.repaint();
	}

	
	public JComboBox<String> getAttributesComboBox1() {
		return attributesComboBox1;
	}

	public JComboBox<String> getAttributesComboBox2() {
		return attributesComboBox2;
	}


	public JComboBox<String> getOperatorComboBox() {
		return operatorComboBox;
	}


	public JoinField(AbstractPlanOperation operation, JPanel panel, ArrayList<JoinField> fields){
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
		attributesComboBox1.setBorder(null);
		attributesComboBox2.setBorder(null);
		operatorComboBox.setBorder(null);
		this.add(attributesComboBox1);
		this.add(operatorComboBox);
		this.add(attributesComboBox2);
		
		this.add(deleteButton);
	
	}
	
	
	public void updateOnClick(AbstractPlanOperation operation){
		for (int i = 0; i < attributesComboBox1.getComponentCount(); i++) {
			Component component = attributesComboBox1.getComponent(i);
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
		
		for (int i = 0; i < attributesComboBox2.getComponentCount(); i++) {
			Component component = attributesComboBox2.getComponent(i);
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
	
	private void setSelected(JComboBox<?> comboBox, Object value){
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