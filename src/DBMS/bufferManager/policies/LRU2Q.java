package DBMS.bufferManager.policies;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.IPage;



public class LRU2Q extends AbstractBufferPolicy{

	
	protected List<IPage> listIn = Collections.synchronizedList(new ArrayList<IPage>());
	protected List<IPage> listOut = Collections.synchronizedList(new ArrayList<IPage>());
	protected List<IPage> listHot = Collections.synchronizedList(new ArrayList<IPage>());
	
	protected int listInSize;
	protected int listOutSize;
	protected int listHotSize;
	
	
	public LRU2Q(Integer capacity){
		super(capacity);
		listInSize = (int) (capacity / 4);
		listHotSize = (int) (capacity * 0.4);
		listOutSize = (int) (listHotSize * 0.5);
		
		listHotSize += (int) (capacity * 0.15);
//		listColdSize = (int) (capacity * 0.15);
	
//		System.out.println("capacity: " + capacity);
//		System.out.println("listInSize: " + listInSize);
//		System.out.println("listOutSize: " + listOutSize);
//		System.out.println("listHotSize: " + listHotSize);
		System.out.println();		
	}
	
	
	
	

	public synchronized IPage find(String pageId) {
		
		super.numberOfOperation++;

		for (int i = 0; i < listIn.size(); i++) {
			IPage page = listIn.get(i);
			if (page != null && page.getPageId().equals(pageId)) {
				hit(page);
				return page;
			}
		}
		
		for (int i = 0; i < listOut.size(); i++) {
			IPage page = listOut.get(i);
			if (page != null && page.getPageId().equals(pageId)) {
				hitMoveToHot(listOut,page);
				return page;
			}
		}
		
		for (int i = 0; i < listHot.size(); i++) {
			IPage page = listHot.get(i);
			if (page != null && page.getPageId().equals(pageId)) {
				hitMoveToHot(listHot,page);
				return page;
			}
		}
		super.missCount++;

		return null;
	}
		
	protected void hit(IPage p){
		super.hitCount++;
		p.addHitCount();
		if(policyListener!=null)policyListener.hit(p);
	}
	
	
	protected void hitMoveToHot(List<IPage> list ,IPage p){
		action(() -> {
			hit(p);	
			list.remove(p);
			insertHot(p);
		});
		
		
	}
	
	
	public void insert(IPage p) {
		action(() -> {
			alloc(p);
			insertIn(p);
		});
	}
	
	
	public void insertIn(IPage p){
		action(() -> {
			
			if(listIn.size() == listInSize){
				insertOut(listIn.remove(0));
			}
			
			listIn.add(p);
			
		});
	
	}

	public void insertOut(IPage p) {
		action(() -> {
			
			if(listOut.size() == listOutSize){
				remove(listOut.get(0));
				
			}
			listOut.add(p);
			
		});
	}

	public void insertHot(IPage p) {
		action(() -> {
			
			if(listHot.size() == listHotSize){
				remove(listHot.get(0));
			}
			
			listHot.add(p);
			
		});
	}


	
	
	public void remove(IPage p) {
		
		action(() -> {
				
				if(!listOut.remove(p)){
					if(!listHot.remove(p)){
						if(!listIn.remove(p)){
							Kernel.log(this.getClass(),"Page "+p.getPageId()+" to be removed not found",Level.SEVERE);
						}
					}
				}
					
			free(p);
		});
		
	}
	
	public String toString(){
		return getName();
	}

	

	@Override
	public String getName() {
		
		return "2Q: A Low Overhead High Performance Buffer Management Replacement Algorithm";
	}


	@Override
	public List<IPage> getPages() {
		List<IPage> newList = Collections.synchronizedList(new ArrayList<IPage>());

		action(() -> {
			
			newList.addAll(listIn);
			newList.addAll(listOut);
			newList.addAll(listHot);
			
		});

		return newList;
	}
	
	public void savePage() {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
		String date = dt.format(new Date());

		try {
			PrintWriter logRequests = new PrintWriter(new FileWriter(new File(date + " LRU2Q-IN"  + ".pages"), true));
			for (IPage p : listIn) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " LRU2Q-OUT"  + ".pages"), true));
			for (IPage p : listOut) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
			logRequests = new PrintWriter(new FileWriter(new File(date + " LRU2Q-HOT"  + ".pages"), true));
			for (IPage p : listHot) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void setPolicyListener(BufferPolicyListener listener) {
		policyListener = listener;
		
	}

	@Override
	protected void logicRemoveAll() {
		action(() -> {
			listIn.clear();
			listOut.clear();
			listHot.clear();			
		});
	}





	@Override
	public int getCurrentNumberOfPages() {
		return listIn.size() + listOut.size() + listHot.size();
	}


}
