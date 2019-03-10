package graphicalInterface.draw;


import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.text.DecimalFormat;
import java.util.ArrayList;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;
import graphicalInterface.util.MeshLayout;


public class DrawBuffer extends JSplitPane{

	private static final long serialVersionUID = 1L;
	
	private JLabel capacity = new JLabel();
	private JLabel missCount = new JLabel();
	private JLabel hitCount = new JLabel();
	private JLabel numberOfOperation = new JLabel();
	private DecimalFormat fmt = new DecimalFormat("0.00"); 
	private ArrayList<DrawPage> lastReplacedPages = new ArrayList<DrawPage>();
	private DrawPage[] memoryPages;
	private JPanel replaced = new JPanel();
	private JPanel memoryPanel;
	private Dimension dimension = new Dimension(100, 100);
	
	public DrawBuffer() {
		super();
		
		super.setOrientation(JSplitPane.VERTICAL_SPLIT);
		super.setRightComponent(policy());
		super.setLeftComponent(infos());
		this.setResizeWeight(0.55);
		
		
	}
	
	public JPanel infos(){
		JPanel infos = new JPanel();
		infos.setBackground(Color.WHITE);
		infos.setLayout(new BoxLayout(infos, BoxLayout.Y_AXIS));
		
		JPanel statisticsPanel = new JPanel();
		statisticsPanel.setBackground(Color.white);
		
		statisticsPanel.setLayout(new MeshLayout(4, 1));
		statisticsPanel.add(capacity);
		statisticsPanel.add(missCount);
		statisticsPanel.add(hitCount);
		statisticsPanel.add(numberOfOperation);
		infos.add(statisticsPanel);
		
	
		memoryPanel = new JPanel();
		memoryPanel.setBackground(Color.white);
		MeshLayout meshLayout =  new MeshLayout(50, 13);
		memoryPanel.setLayout(meshLayout);
		
		
		memoryPages = new DrawPage[Kernel.getBufferManager().getBufferPolicy().getCapacity()];
		for (int i = 0; i < memoryPages.length; i++) {
			memoryPages[i] = new DrawPage();
			memoryPages[i].setPage(null);
			memoryPages[i].setPreferredSize(dimension);
			memoryPanel.add(memoryPages[i]);
		}
	
		JPanel auxMemoryPanel = new JPanel();
		auxMemoryPanel.add(Box.createHorizontalGlue());
		auxMemoryPanel.add(memoryPanel);
		auxMemoryPanel.add(Box.createHorizontalGlue());
		JScrollPane scrollPane = new JScrollPane(auxMemoryPanel);
		
		infos.add(scrollPane);
		updateStatistics();
		
		
		replaced.setLayout(new GridLayout(1, 4));
		//JScrollPane strollLastPages = new JScrollPane(replaced);
		//infos.add(label);
		//infos.add(strollLastPages);
		return infos;
	}
	
	public void allocOrFreeMemoryPage(int position, IPage page){
		if(position>=0) {
			memoryPages[position].setPage(page);
				
		}
	}
	public void updateMemoryPage(int position){
		if(position>=0) {
			 memoryPages[position].update();
		}
	}
	
	public JPanel policy(){
		JPanel policyPanel = new JPanel();
		policyPanel.setBackground(Color.WHITE);
		policyPanel.setLayout(new GridLayout(2, 1));
		JLabel label = new JLabel(Kernel.getBufferManager().getBufferPolicy().getName());
		label.setHorizontalAlignment(JLabel.CENTER);
		policyPanel.add(label);
		
		DrawLRUBufferPolicy drawLRUBufferPolicy = new DrawLRUBufferPolicy(this);
		Kernel.getBufferManager().getBufferPolicy().setPolicyListener(drawLRUBufferPolicy);
		policyPanel.add(drawLRUBufferPolicy);
		return policyPanel;
	}
	
	public void addReplacedPage(DrawPage drawPage){
		if(lastReplacedPages.size() == 3){
			replaced.remove(lastReplacedPages.remove(0));
		}
		lastReplacedPages.add(drawPage);
		replaced.add(drawPage);
		replaced.revalidate();
		replaced.repaint();
	}
	
	

	public void updateStatistics(){
		capacity.setText("Capacity: "+Kernel.getBufferManager().getBufferPolicy().getCapacity());
		missCount.setText("Miss Count: " + Kernel.getBufferManager().getBufferPolicy().getMissCount());
		hitCount.setText("Hit Count: " + Kernel.getBufferManager().getBufferPolicy().getHitCount()+" / " + fmt.format(((double)(Kernel.getBufferManager().getBufferPolicy().getHitCount()*100)/Kernel.getBufferManager().getBufferPolicy().getNumberOfOperation()))+"%");
		if(Kernel.getBufferManager().getBufferPolicy().getHitCount()==0){
			hitCount.setText("Hit Count: 0 / 0%");
		}
		numberOfOperation.setText("Operations Count: " + Kernel.getBufferManager().getBufferPolicy().getNumberOfOperation());	
	
	}

	public Dimension getDimension() {
		return dimension;
	}
	

}
