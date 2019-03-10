package DBMS.bufferManager.policies;

import DBMS.bufferManager.IPage;

public class FIFO extends LRU{

	public FIFO(Integer capacity) {
		super(capacity);
	}

	protected void hitMovePage(IPage p){
		super.hitCount++;
		p.addHitCount();
		if(policyListener!=null)policyListener.updatePage(p);
	}
	
	public String getName() {
		
		return "First In First Out (FIFO)";
	}

}
