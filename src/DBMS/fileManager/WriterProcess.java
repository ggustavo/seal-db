package DBMS.fileManager;

import DBMS.Kernel;
import DBMS.bufferManager.IBufferManager;
import DBMS.bufferManager.IPage;

public class WriterProcess implements Runnable{
	
	private static int TIME = 9000;
	private static boolean RUN = false;
	
	public static boolean ENABLE = false;
	
	public void start(){
		if(RUN == false){
			new Thread(this).start();			
		}
	}
	
	
	public void stop(){
		RUN = false;
	}

	@Override
	public void run() {
		RUN = true;
		synchronized (this) {	
			while(RUN){
				
				if(ENABLE)freePages();
				
				try {
					wait(TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}
		}
	}
	
	
	public synchronized void freePages(){
//		if(Kernel.getScheduler().getWaitForGraph().getNodes().size() > 1){
//			return;
//		}
		
		IBufferManager bufferManager = Kernel.getBufferManager();	
		
		if( bufferManager.getBufferPolicy().getPages().size() >=  bufferManager.getBufferPolicy().getCapacity() - ((int)bufferManager.getBufferPolicy().getCapacity()/6)  ){
			//LogError.save(this.getClass(),"DB WRITER!");
			for (int i = 0; i < (int) bufferManager.getBufferPolicy().getCapacity()/6; i++) {
				
				IPage p = bufferManager.getBufferPolicy().getPages().get(i);
				if(p!=null){
					
					bufferManager.getBufferPolicy().remove(p);
				}
				
			}
			
		}
	}
	
}
