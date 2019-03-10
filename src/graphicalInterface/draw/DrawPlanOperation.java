package graphicalInterface.draw;

import java.awt.Cursor;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.ImageIcon;
import javax.swing.JLabel;


import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import graphicalInterface.Events;


public class DrawPlanOperation extends JLabel{

	
	
	private static final long serialVersionUID = 1L;
	private AbstractPlanOperation planOperation;
	private DrawPlan drawPlan;
	private JLabel labelName;
	private ImageIcon deselect;
	boolean isSelected;
	
	public DrawPlanOperation(DrawPlan drawPlan,AbstractPlanOperation planOperation, ImageIcon deselect,ImageIcon select) {
		this.deselect = deselect;
		this.setIcon(deselect);
		this.planOperation = planOperation;
		this.setDrawPlan(drawPlan);
		this.addMouseListener(new MouseListener() {
		
			public void mouseReleased(MouseEvent arg0) {
				
				
			}
			
			
			public void mousePressed(MouseEvent arg0) {
				
				
				
			}
			
			
			public void mouseExited(MouseEvent arg0) {			
				if(!Events.clickButton)setCursor( Cursor.getPredefinedCursor( Cursor.HAND_CURSOR));
				//Events.setMouseNormalIcon();	
				//if(deselect!=null)setIcon(deselect);
			}
			
			
			public void mouseEntered(MouseEvent arg0) {
				//if(select!=null)setIcon(select);
				
			}
			
			public void mouseClicked(MouseEvent arg0) {
				if(Events.clickButton){
					AbstractPlanOperation c = null;
					try {
						c = (AbstractPlanOperation) Events.classOperation.newInstance();
					} catch (InstantiationException e) {
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					}
					
					drawPlan.getPlan().addOperation(planOperation, c);
					drawPlan.drawPlan(drawPlan.getPlan());
					drawPlan.getMenuOptions().showInitialMenu();
					Events.setMouseNormalIcon(drawPlan.getPanelArea());
				}else{
					addMenu();
					if(select!=null)setIcon(select);
				}
				
			}
		});
	}
	
	public void deselect(){
		if(deselect!=null)setIcon(deselect);
	}
	
	public void addMenu(){
		drawPlan.getMenuOptions().showMenu(this);
	}
	
	public AbstractPlanOperation getPlanOperation() {
		return planOperation;
	}


	public void setPlanOperation(AbstractPlanOperation planOperation) {
		this.planOperation = planOperation;
	}


	public DrawPlan getDrawPlan() {
		return drawPlan;
	}


	public void setDrawPlan(DrawPlan drawPlan) {
		this.drawPlan = drawPlan;
	}

	public JLabel getLabelName() {
		return labelName;
	}

	public void setLabelName(JLabel labelName) {
		this.labelName = labelName;
	}
	
	
	
}
