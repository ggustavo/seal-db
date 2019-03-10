package DBMS.bufferManager;

import java.util.ArrayList;
import java.util.Collection;

import DBMS.queryProcessing.ITuple;

public class Page implements IPage {

	private byte[] data;
	private char type;
	private String pageId;
	private int hitCount;
	private int memoryPosition = - 1;
	private Collection<Page> repository;
	private ArrayList<ITuple> tuplesCache = null;


	public byte[] getData() {
		

			return data;
		
			///return data.clone();  TODO: Tests
			
		
	}

	public void setData(byte[] data) {
		this.data = data;
	}


	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}
	
	public String toString(){
		return type+"("+pageId+")";
	}

	public int getHitCount() {
		return hitCount;
	}

	public void addHitCount() {
		this.hitCount++;
	}

	public int getMemoryPosition() {
		return memoryPosition;
	}

	public void setMemoryPosition(int memoryPosition) {
		this.memoryPosition = memoryPosition;
	}

	public Collection<Page> getRepository() {
		return repository;
	}

	public void setRepository(Collection<Page> repository) {
		this.repository = repository;
	}

	public synchronized ArrayList<ITuple> getTuplesCache() {
		return tuplesCache;
	}

	public synchronized void setTuplesCache(ArrayList<ITuple> tuplesCache) {
		this.tuplesCache = tuplesCache;
	}
	
	
	
	
}
