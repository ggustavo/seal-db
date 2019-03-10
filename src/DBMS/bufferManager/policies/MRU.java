package DBMS.bufferManager.policies;

import DBMS.bufferManager.IPage;

public class MRU extends LRU{

	public MRU(Integer capacity) {
		super(capacity);
	}

	public void replacement(){
		IPage p = list.get(list.size()-1);
		//LogError.save(this.getClass(),"REMOVED: " +p.getPageId());
		remove(p);
	}
	
	public String getName() {
		
		return "Most Recently Used (MRU)";
	}


}
