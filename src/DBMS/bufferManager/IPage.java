package DBMS.bufferManager;

import java.util.ArrayList;
import java.util.Collection;

import DBMS.queryProcessing.ITuple;

public interface IPage {
	
	public static char READ_PAGE = 'R';
	public static char WRITE_PAGE = 'W';
	
	byte[] getData();

	void setData(byte[] data);

	char getType();

	void setType(char type);

	String getPageId();

	void setPageId(String pageId);

	String toString();

	int getHitCount();

	void addHitCount();

	int getMemoryPosition();

	void setMemoryPosition(int memoryPosition);
	
	static IPage getInstance(){
		return new Page();
	}
	
	public Collection<Page> getRepository();

	public void setRepository(Collection<Page> repository);
	
	public ArrayList<ITuple> getTuplesCache();
	

	public void setTuplesCache(ArrayList<ITuple> tuplesCache);

}