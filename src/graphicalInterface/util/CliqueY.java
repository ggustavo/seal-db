package graphicalInterface.util;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;

import javax.swing.JPanel;

public class CliqueY implements MouseMotionListener, MouseListener {
	JPanel panel;
	int dX=  0;
	int dY = 0;
	boolean possoMover;
	
	public CliqueY(JPanel j){
		this.panel = j;
	}
	
	@Override
	public void mouseDragged(MouseEvent e) {

		if (possoMover) {
	           panel.setLocation(panel.getX(), e.getLocationOnScreen().y - dY);
	           
	           	dX = e.getLocationOnScreen().x - panel.getX();
	           dY = e.getLocationOnScreen().y - panel.getY();
	      }
	}
	@Override
	public void mouseMoved(MouseEvent e) {		}
	@Override
	public void mouseClicked(MouseEvent e) {	}
	@Override
	public void mouseEntered(MouseEvent e) {}
	@Override
	public void mouseExited(MouseEvent e) {}
	@Override
	public void mouseReleased(MouseEvent e) {
		possoMover = false;
	}
	@Override
	public void mousePressed(MouseEvent e) {
		
		if (panel.contains(e.getPoint())) {
			dX = e.getLocationOnScreen().x - panel.getX();
            dY = e.getLocationOnScreen().y - panel.getY();
           possoMover = true;
            
        }
        
		
	}
	
	
	
}