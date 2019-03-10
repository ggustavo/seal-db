package graphicalInterface.util;

import java.awt.Graphics;
import java.awt.Image;

import javax.swing.JPanel;

public class ImagePanel extends JPanel {

	private static final long serialVersionUID = 1L;
	
	private Image image;
	private boolean size = false;
	private int w;
	private int h;

	public ImagePanel(Image image) {
		super();
		this.image = image;
		
	}
	
	public ImagePanel(Image image, int w, int h) {
		super();
		this.image = image;
		this.size = true;
		this.w = w;
		this.h = h;
		
		
	}

	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		
		if(size){
			g.drawImage(image, 0, 0, w, h, this); 
		}else{
			
			if(this.getWidth() <= this.getHeight() ){
				g.drawImage(image, 0, 0, this.getWidth(), this.getWidth(),this); 	
			}else{
				g.drawImage(image, 0, 0,this.getHeight() , this.getHeight(),this); 
			}
			
		}
								
	}

}