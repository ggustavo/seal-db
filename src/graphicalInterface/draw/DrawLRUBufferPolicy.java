package graphicalInterface.draw;

import java.awt.Graphics;
import java.awt.GridLayout;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import javax.swing.JPanel;
import javax.swing.JScrollPane;


import DBMS.Kernel;
import DBMS.bufferManager.IPage;
import DBMS.bufferManager.policies.BufferPolicyListener;
import DBMS.bufferManager.policies.FIFO;





public class DrawLRUBufferPolicy extends JScrollPane implements BufferPolicyListener{

	
	
	private static final long serialVersionUID = 1L;
	private JPanel internalPane;
	private HashMap<IPage, DrawPage> hashDrawPages;
	private List<DrawPage> drawPages;
	private DrawBuffer drawBuffer;
	

	public DrawLRUBufferPolicy(DrawBuffer drawBuffer) {
		super();
		this.drawBuffer = drawBuffer;
		this.internalPane = new JPanel();
		this.internalPane.setLayout(new GridLayout(1,80));
		this.setViewportView(internalPane);
		this.hashDrawPages = new HashMap<IPage, DrawPage>();
		this.drawPages = new LinkedList<>();
	}

	public void insert(IPage page){
		DrawPage dpage = new DrawPage(page);
		dpage.setPreferredSize(drawBuffer.getDimension());
		hashDrawPages.put(page, dpage);
		drawPages.add(dpage);
		internalPane.add(dpage);
		updateTempColor();
		
	}
	
	public void remove(IPage page){
		
		DrawPage p = hashDrawPages.get(page);
		if (p != null) {
			internalPane.remove(hashDrawPages.get(page));
			hashDrawPages.remove(page);
			drawPages.remove(p);
			updateTempColor();
	
		}
		
		
	}
	
	public  synchronized void updateTempColor(){
		if(Kernel.getBufferManager().getBufferPolicy() instanceof FIFO){
			return;
		}
//		SwingUtilities.invokeLater(new Runnable() {
//			
//			@Override
//			public void run() {
//				
//				
//			}
//		});
		
		try{
			int p = 0;
			for (int i= 0; i < drawPages.size();i++) {
				if(i < drawPages.size()){
					if(drawPages == null)Kernel.log(this.getClass(),"DRAW PAGES NULL",Level.SEVERE);
					DrawPage drawPage = drawPages.get(i); 
					drawPage.setTempColor(drawPages.size(), p++);				
				}
			}
		}catch (Exception e) {
			Kernel.exception(this.getClass(),e);
		}
		revalidate();
		repaint();	
		
	
	}
	
	
	public void setLastRemoved(IPage page){
		drawBuffer.addReplacedPage(new DrawPage(page));
	}
	
	public void hit(IPage page){
		//this.getHorizontalScrollBar().setValue(this.hashDrawPages.get(page).getX());		
		//LogError.save(this.getClass(),page.getMemoryPosition());
		//LogError.save(this.getClass(),page.getPageId());
		drawBuffer.updateMemoryPage(page.getMemoryPosition());
	}
	
	public void updatePage(IPage page){
		this.hashDrawPages.get(page).update();
		drawBuffer.updateMemoryPage(page.getMemoryPosition());
	}
	

	public void paint(Graphics g) {
		drawBuffer.updateStatistics();
			super.paint(g);
		}

	@Override
	public void alloc(IPage page) {
		drawBuffer.allocOrFreeMemoryPage(page.getMemoryPosition(), page);
		
	}

	@Override
	public void free(IPage page) {
		drawBuffer.allocOrFreeMemoryPage(page.getMemoryPosition(), null);
		
	}
		
	
		
}
