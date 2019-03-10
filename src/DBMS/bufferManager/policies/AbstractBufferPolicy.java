package DBMS.bufferManager.policies;


import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.BufferManager;
import DBMS.bufferManager.IPage;
import DBMS.fileManager.dataAcessManager.DataAcess;

public abstract class AbstractBufferPolicy {

	
	protected int capacity;
	protected long missCount;
	protected long hitCount;
	protected long numberOfOperation;
	protected BufferPolicyListener policyListener;
	protected IPage[] pages;
	protected List<Integer> freePages;
	protected boolean inMemoryMode = false;

	

	public AbstractBufferPolicy(Integer capacity) {
		this.capacity = capacity;
		pages = new IPage[capacity];
		freePages = new LinkedList<>();
		for (int i = 0; i < pages.length; i++) {
			freePages.add(i);
			
		}	
	}
	
	public void resetStatistics(){
		missCount = 0;
		hitCount = 0;
		numberOfOperation = 0;
	}
	
	protected void reset(){
		logicRemoveAll();
		freePages.clear();
		
		pages = new IPage[capacity];
		for (int i = 0; i < pages.length; i++) {
			freePages.add(i);	
		}	
		
	}

	public abstract void setPolicyListener(BufferPolicyListener listener);
	public abstract IPage find(String pageId);
	
	public abstract void insert(IPage p);
	public abstract void remove(IPage p);
	
	protected abstract void logicRemoveAll();
	
	public abstract String getName();
	public abstract List<IPage> getPages();
	
	public abstract int getCurrentNumberOfPages();

	public synchronized boolean action(Runnable r){
		r.run();
		return true;
	}
	
	
	public void flush(IPage p) {
		action(() -> {
		
			if (p.getType() == IPage.WRITE_PAGE) {
				DataAcess.writeBlock(p.getPageId(), p.getData());
			}
		});
	}

	public void flushAll() {
		action(() -> {
			
			for (IPage page : getPages()) {

				flush(page);

			}
		});
	}
	
	public void freeAll(){
		action(() ->{
	
			flushAll();
			for (int i = 0; i < pages.length; i++) {
				if(pages[i] != null)free(pages[i]);
			}
			reset();
			
		});
	}
	
	public void alloc(IPage page){
		action(() ->{
		if(page.getMemoryPosition() == -1){
			
			boolean allocated = false;
			if(freePages.size() == 0){
				Kernel.log(AbstractBufferPolicy.class, "[ERR0] buffer full", Level.SEVERE);
				return;
			}
			int postion = freePages.get(0);
			IPage pFree = pages[postion];
			if(pFree == null){
				pages[postion] = page;
				page.setMemoryPosition(postion);
				allocated = true;
				freePages.remove(0);
			}else{
				Kernel.log(AbstractBufferPolicy.class, "[ERR0] buffer aloc error", Level.SEVERE);
			}
			
			if(policyListener!=null && allocated)policyListener.alloc(page);			
		}else {
			
		}
		});
	}
	
	protected void free(IPage page){
		action(() ->{
		
			if(inMemoryMode && Kernel.getBufferManager().getColdDataState() == BufferManager.STATE_LOADING_COLD_FINISH) {
				((BufferManager)Kernel.getBufferManager()).insertInCold(page);
			}else {
				flush(page);
				pages[page.getMemoryPosition()] = null;
				freePages.add(page.getMemoryPosition());
			}
			if(policyListener!=null)policyListener.free(page);		
		
		});
	}

	public int getCapacity() {
		return capacity;
	}

	public long getMissCount() {
		return missCount;
	}

	public long getHitCount() {
		return hitCount;
	}

	public long getNumberOfOperation() {
		return numberOfOperation;
	}
	public void savePage() {}

	public boolean isInMemoryMode() {
		return inMemoryMode;
	}

	public void setInMemoryMode(boolean inMemoryMode) {
		this.inMemoryMode = inMemoryMode;
	}

	
	
}
