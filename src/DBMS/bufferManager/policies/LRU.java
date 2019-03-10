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

import DBMS.bufferManager.IPage;



public class LRU extends AbstractBufferPolicy{

	public LRU(Integer capacity) {
		super(capacity);
	}

	protected List<IPage> list = Collections.synchronizedList(new ArrayList<IPage>());


	public synchronized IPage find(String pageId) {
			super.numberOfOperation++;
			
			for (int i = 0; i < list.size(); i++) {
				IPage page = list.get(i);
				if(page != null && page.getPageId().equals(pageId)){
					hitMovePage(page);
					return page;
				}
			}
			
//			IPage page = getHash().get(pageId);
//			if(page != null && page.getPageId().equals(pageId)){
//				hitMovePage(page);
//				return page;
//			}
			
			super.missCount++;
			
	
		return null;
	}
	
	
	protected void hitMovePage(IPage p){
		
		action(() -> {

			super.hitCount++;
			p.addHitCount();
			
			list.remove(p);
			if(policyListener!=null)policyListener.remove(p);
			
			
			list.add(p);
			
			if(policyListener!=null)policyListener.insert(p);				
			if(policyListener!=null)policyListener.hit(p);
		});
		

		
	}
	
	
	public void insert(IPage p) {
		
		
		action(() -> {
			if(list.size() == super.capacity){
				replacement();
			}
			alloc(p);
			list.add(p);
			
			if(policyListener!=null)policyListener.insert(p);	
		});
		

	}
	
	public void replacement(){
		
		action(() -> {
			IPage p = list.get(0);
			remove(p);
		});
	}
	
	
	public void remove(IPage p) {
		action(() -> {
			list.remove(p);
			free(p);
			if(policyListener!=null) policyListener.remove(p);
			if(policyListener!=null) policyListener.setLastRemoved(p);
		});
		
		
	}
	
	public String toString(){
		return list.toString();
	}

	

	@Override
	public String getName() {
		
		return "Least Recently Used (LRU)";
	}


	@Override
	public List<IPage> getPages() {
		return list;
	}


	@Override
	public void setPolicyListener(BufferPolicyListener listener) {
		policyListener = listener;
		
	}

	@Override
	protected void logicRemoveAll() {
		action(() -> {
			list.clear();	
		});
	}


	public void savePage() {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
		String date = dt.format(new Date());
		
		try {
			PrintWriter logRequests = new PrintWriter(new FileWriter(new File(date+" "+getName()+".pages") ,  true));
		
			for (IPage p : getPages()) {
				logRequests.println(p.getPageId()+" ["+p.getType()+"]");
			}
			
			
			logRequests.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	@Override
	public int getCurrentNumberOfPages() {
		return list.size();
	}


}
