package tests;

import DBMS.Kernel;
import DBMS.fileManager.catalog.InitializerListen;
import DBMS.memoryManager.algo.ARC;
import DBMS.memoryManager.algo.LRU;
import DBMS.memoryManager.algo.LRU2Q;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Transaction;

public class TestTPCC {
	
	public static void main(String[] args) {
		try {
			
			Kernel.ENABLE_RECOVERY = true;	
			Kernel.ENABLE_LOG_REQUESTS = false;
			Kernel.ENABLE_HOT_COLD_DATA_ALGORITHMS = false;
			Kernel.MEMORY_SIZE_TUPLES = 100000;
			
			Kernel.getInitializer().setInitializerListen(new InitializerListen() {
				
				@Override
				public void afterStartSystemCatalog(Transaction systemTransaction) {
					try {
						TPCCLoad.createSchema(systemTransaction);
					} catch (AcquireLockException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
					TPCCLoad.exec(systemTransaction, null);
				}

			});
		//	Kernel.getMemoryAcessManager().setAlgorithm(new LRU());
		//	Kernel.getMemoryAcessManager().setAlgorithm(new ARC());
		//	Kernel.getMemoryAcessManager().setAlgorithm(new LRU2Q());
			Kernel.start(); 
			
			TransactionsThroughput.start();
			
			TPCCBenchmark benchmark = new TPCCBenchmark();
			benchmark.numberOfTransactions = 20;
			benchmark.serial = false;	
			benchmark.startBenchmark();
			
			
		}catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		

	}
	
}
