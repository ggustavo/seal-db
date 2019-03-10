package DBMS.bufferManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.bufferManager.policies.AbstractBufferPolicy;
import DBMS.bufferManager.policies.LRU;
import DBMS.fileManager.dataAcessManager.DataAcess;
import DBMS.transactionManager.ITransaction;



public class BufferManager implements IBufferManager {
	
	private AbstractBufferPolicy bufferPolicy;
	private AbstractBufferPolicy tempBufferPolicy;
	
	private HashMap<String, IPage> coldData;


	public static int STATE_OFF = 0;
	public static int STATE_LOADING_COLD = 1;
	public static int STATE_LOADING_COLD_FINISH = 2;
	
	private int coldDataState = STATE_OFF;
	
	public void setEnableColdDataLoad(boolean enableColdList) {
		
		
		if(coldData == null) {
			coldDataState = STATE_LOADING_COLD;
			coldData = new HashMap<>();
		}else {
			coldDataState = STATE_LOADING_COLD_FINISH;
			Kernel.log(Kernel.class, coldData.size()+" pages loaded in-memory",Level.CONFIG);
		}
		
	}
	
	private PrintWriter logRequests;
	private BigInteger time = BigInteger.ZERO;
	
	public void closeLog() {
		if(logRequests!=null)logRequests.close();
	}
	
	
	public BufferManager() {}
	
	public void startTempBufferPolicy(){
		if(Kernel.ENABLE_TEMP_BUFFER) {
			tempBufferPolicy = new LRU(Kernel.BUFFER_SIZE_TEMP);
			tempBufferPolicy.setInMemoryMode(false);
		}
	}
	

	public synchronized void insertInCold(IPage page) {
		if(bufferPolicy.getCurrentNumberOfPages() + coldData.size() >= Kernel.BUFFER_SIZE) {
			Kernel.log(Kernel.class, "OUT OF MEMORY -- Try Flush Page "+page.getPageId()+ " ("+ page.getType()+")",Level.SEVERE);
			bufferPolicy.flush(page);
		}else {
			coldData.put(page.getPageId(), page);			
		}
	}
	
	public synchronized IPage removeFromCold(String id) {
		return coldData.remove(id);
	}
	

	private synchronized void saveLog(String pageId, char operation) {
		
		if(logRequests==null) {
			
			try {
				SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
				logRequests = new PrintWriter(new FileWriter(new File("log_requests "+dt.format(new Date())+".requests") ,  true));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
			
		logRequests.println(time.toString()+":"+pageId+" ["+operation+"]");
		time.add(BigInteger.ONE);
		
	}

	
	public synchronized IPage getPage(ITransaction transaction,String pageId, char operation, boolean isTemp){
		
		
		//SALVAR NO ARQUIVO 
		
		//operation+":"+pageId":"time
		
		if(!isTemp && Kernel.ENABLE_LOG_BUFFER_REQUESTS)saveLog(pageId, operation);
		
		IPage page = null;
		
		
		if(Kernel.ENABLE_LOAD_ALL_PAGES_IN_MEMORY_MODE && coldDataState == STATE_LOADING_COLD && !isTemp) {
			page = readNewPage(transaction, pageId,isTemp);
			insertInCold(page);
			page.setMemoryPosition(0);
			return page;
		}
		
	
		page = Kernel.ENABLE_TEMP_BUFFER && isTemp ? tempBufferPolicy.find(pageId) : bufferPolicy.find(pageId);
		
		if (page == null) {
			final IPage newPage = readNewPage(transaction, pageId,isTemp);
			if(newPage == null) {
				System.out.println("NULL: " + pageId + " " + isTemp);
			}
			newPage.setType(operation);
			
			
			if(Kernel.ENABLE_TEMP_BUFFER && isTemp){
				tempBufferPolicy.insert(newPage);
			}else{
				bufferPolicy.insert(newPage);
			}
			
			return newPage;
		} 
		return page;
	}


	
	private IPage readNewPage(ITransaction transaction,String pageId, boolean isTemp){
			
			IPage page = null;
		
			if(Kernel.ENABLE_LOAD_ALL_PAGES_IN_MEMORY_MODE && coldDataState == STATE_LOADING_COLD_FINISH && !isTemp) {
				return removeFromCold(pageId);
			}else {
				page = IPage.getInstance();
				page.setData(DataAcess.readBlock(pageId));
				page.setType(IPage.READ_PAGE);
				page.setPageId(pageId);	
			}
		return page;
	}
	
	public void flush(){
		bufferPolicy.flushAll();
	}

	public void freeAll(){
		bufferPolicy.freeAll();
	}
	
	public void resetStatistics(){
		bufferPolicy.resetStatistics();
	}
	
	
	public AbstractBufferPolicy getBufferPolicy() {
		return bufferPolicy;
	}
	

	public void setBufferPolicy(AbstractBufferPolicy bufferPolicy) {
		this.bufferPolicy = bufferPolicy;
		if(Kernel.ENABLE_LOAD_ALL_PAGES_IN_MEMORY_MODE)bufferPolicy.setInMemoryMode(true);
	}


	public int getColdDataState() {
		return coldDataState;
	}

	public void saveColdData() {
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
		String date = dt.format(new Date());

		try {
			PrintWriter logRequests = new PrintWriter(new FileWriter(new File(date + " COLD-DATA"  + ".pages"), true));
			for (IPage p : coldData.values()) {
				logRequests.println(p.getPageId() + " [" + p.getType() + "]");
			}
			logRequests.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
