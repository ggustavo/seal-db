package graphicalInterface.draw;

import java.awt.Cursor;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.ImageIcon;
import javax.swing.JLabel;

import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import graphicalInterface.Events;

public class DrawEmptySlot extends JLabel{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private AbstractPlanOperation planOperation;
	private DrawPlan drawPlan;
	
	public DrawEmptySlot(DrawPlan drawPlan,AbstractPlanOperation planOperation, ImageIcon deselect,ImageIcon select, boolean direction){
		this.setIcon(deselect);
		this.setDrawPlan(drawPlan);
		this.addMouseListener(new MouseListener() {
		
			public void mouseReleased(MouseEvent arg0) {
				
				
			}
			
			
			public void mousePressed(MouseEvent arg0) {
				
				
				
			}
			
			
			public void mouseExited(MouseEvent arg0) {			
				if(!Events.clickButton)setCursor( Cursor.getPredefinedCursor( Cursor.HAND_CURSOR));
				if(deselect!=null)setIcon(deselect);
			}
			
			
			public void mouseEntered(MouseEvent arg0) {
				if(select!=null)setIcon(select);
				
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
					if(planOperation!=null){
						
						drawPlan.getPlan().addOperationDown(direction, planOperation, c);
					}else{
						
						drawPlan.getPlan().addOperation(c);
					}
					drawPlan.drawPlan(drawPlan.getPlan());
					Events.setMouseNormalIcon(drawPlan.getPanelArea());
				}
				
			}
		});
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
	
	
}
