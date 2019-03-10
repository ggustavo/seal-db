package DBMS.bufferManager.policies;

import DBMS.bufferManager.IPage;

public  interface BufferPolicyListener {

	public void insert(IPage page);
	public void remove(IPage page);
	public void hit(IPage page);
	public void updatePage(IPage page);
	public void setLastRemoved(IPage page);
	
	public void alloc(IPage page);
	public void free(IPage page);
}
