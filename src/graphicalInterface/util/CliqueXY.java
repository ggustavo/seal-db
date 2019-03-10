package graphicalInterface.util;


import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;

import javax.swing.JLayeredPane;
import javax.swing.JPanel;

import graphicalInterface.Events;
import graphicalInterface.draw.DrawPlan;


public class CliqueXY implements MouseMotionListener, MouseListener {
	private JPanel panel;
	private JLayeredPane jLayeredPane;
	private int dX= 0;
	private int dY = 0;
	private boolean move;
	private DrawPlan drawPlan;
	private boolean abstractMode = false;
	
	public CliqueXY(JPanel j, JLayeredPane jp){
		this.jLayeredPane = jp;
		this.panel = j;
		
	}
	
	public CliqueXY(JPanel j, DrawPlan drawPlan ){
		this.drawPlan = drawPlan;
		this.panel = j;
		
	}
	
	public CliqueXY(JPanel panel){
		this.panel = panel;
		abstractMode = true;
		
	}
	
	@Override
	public void mouseDragged(MouseEvent e) {

		if (move ) {
	           	panel.setLocation(e.getLocationOnScreen().x - dX, e.getLocationOnScreen().y - dY);
	           	if(jLayeredPane!=null)jLayeredPane.moveToBack(panel);
	            dX = e.getLocationOnScreen().x - panel.getX();
	            dY = e.getLocationOnScreen().y - panel.getY();
	      }
	}
	@Override
	public void mouseMoved(MouseEvent e) {	
		
	}
	
	
	@Override
	public void mouseClicked(MouseEvent e) {
		
		if(abstractMode==false)Events.setMouseNormalIcon(drawPlan.getPanelArea());
	}
	@Override
	public void mouseEntered(MouseEvent e) {}
	@Override
	public void mouseExited(MouseEvent e) {}
	@Override
	public void mouseReleased(MouseEvent e) {
		move = false;
	}
	@Override
	public void mousePressed(MouseEvent e) {
		
		if (panel.contains(e.getPoint())) {
            dX = e.getLocationOnScreen().x - panel.getX();
            dY = e.getLocationOnScreen().y - panel.getY();
           move = true;
            
        }
        
		
	}
	
	
	
}