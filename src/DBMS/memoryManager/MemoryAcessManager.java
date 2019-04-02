package DBMS.memoryManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Schema;
import DBMS.memoryManager.algo.LRU;
import DBMS.memoryManager.algo.Memory;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;


public class MemoryAcessManager {
	
	private Memory algorithm = null;
	
	
	private PrintWriter logRequests;
	private BigInteger time = BigInteger.ZERO;
	private boolean started = false;
	
	public void start() {
		if(!Kernel.ENABLE_HOT_COLD_DATA_ALGORITHMS)return; 
		if(algorithm==null)algorithm = new LRU();
		int count = 0;
		for(Schema s : Kernel.getCatalog().getShemas()) {
			
			for (MTable table : s.getTables()) {
				if(!table.isSystemTable() && !table.isTemp()) {
					for (Tuple tuple : table.getTuples()) {
						algorithm.addCold(tuple);
						count++;
					}
				}

			}		
		}
		
		started = true;
		Kernel.log(this.getClass(),"Load " + count + " tuples, total size: " + algorithm.getSize() + " bytes",Level.SEVERE);
	}

	
	
	public void request(char operation, Tuple tuple) {
		if(!Kernel.ENABLE_HOT_COLD_DATA_ALGORITHMS || !started)return; 
		if(!tuple.getTable().isSystemTable() && !tuple.getTable().isTemp()) {
			algorithm.request(operation, tuple);			
			if(Kernel.ENABLE_LOG_REQUESTS)saveLog(tuple.getFullTupleID(), operation);
		}
		
	}
	

	public Memory getAlgorithm() {
		return algorithm;
	}



	public void setAlgorithm(Memory algorithm) {
		this.algorithm = algorithm;
	}
	
	
	private synchronized void saveLog(String id, char operation) {
		
		if(logRequests==null) {
			
			try {
				SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh-mm");
				logRequests = new PrintWriter(new FileWriter(new File("log_requests "+dt.format(new Date())+".requests") ,  true));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
			
		logRequests.println(time.toString()+":"+id+" ["+operation+"]");
		time.add(BigInteger.ONE);
		
	}

	public void closeLog() {
		if(logRequests!=null)logRequests.close();
	}
	
}
