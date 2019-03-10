package graphicalInterface.draw;


import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import DBMS.bufferManager.IPage;


public class DrawPage extends JPanel{
		
	//public final static Color READ_COLOR = new Color(46, 70, 226);
	//public final static Color WRITE_COLOR = new Color(253, 107, 107);
	public final static Color READ_COLOR = new Color(252, 243, 135);
	public final static Color WRITE_COLOR = new Color(255, 125, 9);
	//
	
	private static final long serialVersionUID = 1L;
		private JLabel id;
		private JLabel hitCount;
		private IPage page;
	
		
		public DrawPage(){
			this.setPreferredSize(new Dimension(60, 60));
			this.setBorder(BorderFactory.createLineBorder(Color.black));
			id = new JLabel();
			hitCount = new JLabel();
			id.setHorizontalAlignment(JLabel.CENTER);
			hitCount.setHorizontalAlignment(JLabel.CENTER);
			this.setLayout(new GridLayout(2, 1));
			this.add(id);
			this.add(hitCount);
		
		}
		public DrawPage(IPage page){
			this();
			this.page = page;
			update();
		}
	
		public void update(){
			if(page==null) {
				setPage(null);
				return;
			}
			if(page.getType() == IPage.READ_PAGE){
				this.setBackground(READ_COLOR);
				this.id.setText("Read("+page.getPageId()+")");
			}else{
				this.setBackground(WRITE_COLOR);
				this.id.setText("Write("+page.getPageId()+")");

			}
			this.hitCount.setText("Hits: "+(page.getHitCount()));
		}
		public void updateHit(){
			this.hitCount.setText("Hit: "+(page.getHitCount()));
		}
		
		public void setPage(IPage page){
			if(page==null){
				this.setBackground(Color.GRAY);
				this.hitCount.setText("");
				this.id.setText("");
			}else{
				this.page = page;
				update();				
			}
		}
		
		public final static Color COLD_COLOR = new Color(46, 70, 226);
		public final static Color HOT_COLOR = new Color(255, 0, 0);
		
		public void setTempColor(int size, int p){
	
			this.setBackground(interpolationColors(COLD_COLOR, HOT_COLOR, p, size));
		}
		
		public Color interpolationColors(Color color1,Color color2,int i,int steps){
			float ratio = (float) i / (float) steps;
            int red = (int) (color2.getRed() * ratio + color1.getRed() * (1 - ratio));
            int green = (int) (color2.getGreen() * ratio + color1.getGreen() * (1 - ratio));
            int blue = (int) (color2.getBlue() * ratio + color1.getBlue() * (1 - ratio));
            return new Color(red, green, blue);
		}
		
		
		public IPage getPage() {
			return page;
		}
		
}
