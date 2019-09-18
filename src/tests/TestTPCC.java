package tests;

import DBMS.Kernel;
import DBMS.fileManager.Schema;
import DBMS.fileManager.catalog.InitializerListen;
import DBMS.memoryManager.algo.ARC;
import DBMS.memoryManager.algo.LRU;
import DBMS.memoryManager.algo.LRU2Q;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.TransactionRunnable;

public class TestTPCC {
	
	public static void main(String[] args) {
		try {
			
			Kernel.ENABLE_RECOVERY = true;	
			Kernel.ENABLE_FAST_RECOVERY_STRATEGIE = true;
			
			Kernel.ENABLE_LOG_REQUESTS = false;
			Kernel.ENABLE_HOT_COLD_DATA_ALGORITHMS = false;
			Kernel.MEMORY_SIZE_TUPLES = 100000;
			
			Kernel.TRANSACTION_NUMBER_OF_WORKERS = 4;

			//Kernel.LOG_STRATEGY = Kernel.ASYCHRONOUS_DOUBLE_INDEXED_LOG;
			Kernel.LOG_STRATEGY = Kernel.ASYCHRONOUS_DOUBLE_INDEXED_RECORD_TREE_LOG;
			//Kernel.LOG_STRATEGY = Kernel.SEQUENTIAL_lOG;
			
			
			Kernel.getInitializer().setInitializerListen(new InitializerListen() {
				
				@Override
				public void afterStartSystemCatalog(Transaction systemTransaction) {
					try {
						TPCCLoad.createSchema(systemTransaction);
					} catch (AcquireLockException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//TPCCLoad.exec(systemTransaction, null);
					
				}

			});
			Kernel.getMemoryAcessManager().setAlgorithm(new LRU());
		//	Kernel.getMemoryAcessManager().setAlgorithm(new ARC());
		//	Kernel.getMemoryAcessManager().setAlgorithm(new LRU2Q());
			Kernel.start(); 
			
		
			
			//loadData();
			
			System.out.println();
			for (Schema schema : Kernel.getCatalog().getShemas()) {
				for (MTable table : schema.getTables()) {
					if(table.isTemp() || table.isSystemTable())continue;		
					System.out.println(table.getName() + " " + table.getTuplesHash().size()+"/"+table.getNumberOfTuples(null));
					//table.setNumberOfTuples(table.getTuplesHash().size());
					//Kernel.getInitializer().saveTableSizeMetaData(null, table);
				}
			}
			System.out.println();
//			
			
			
		
			
			//TransactionsThroughput.start();
//			

//////			
			TPCCBenchmark benchmark = new TPCCBenchmark();
			benchmark.numberOfTransactions  = 10;
			benchmark.saveEventsLog = false;
			benchmark.serial = false;
			TPCCBenchmark.debug = false;
			
			benchmark.RANDOM_VALUES = false;
			benchmark.EXECUTE_ALL = false;
			benchmark.startBenchmark();

			
//			Thread.sleep(80000);
//			System.out.println("Flush...");
//			Kernel.getRecoveryManager().forceFlush();
//			
//			Thread.sleep(1000);
//			System.out.println("Close...");
//			Kernel.getRecoveryManager().forceClose();
//			System.out.println("Finish Close");			
			
		}catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		

	}
	
	public static void loadData(){
//		
//		try {
//			Thread.sleep(2000);
//		} catch (InterruptedException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}

		Kernel.getExecuteTransactions().execute(new TransactionRunnable() {
			
			@Override
			public void run(Transaction transaction) throws AcquireLockException {
				
				TPCCLoad.exec(transaction, null);

				transaction.commit();
				System.out.println("Finish Commit Load Transaction");
				Kernel.getRecoveryManager().forceFlush();
			}
			
			@Override
			public void onFail(Transaction transaction, Exception e) {
				e.printStackTrace();
				
			}
		}, true,true,true);  //	}, true,true,true);
	}
	
}
