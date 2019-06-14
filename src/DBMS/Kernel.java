package DBMS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import DBMS.fileManager.catalog.Initializer;
import DBMS.memoryManager.MemoryAcessManager;
import DBMS.fileManager.catalog.CatalogAccess;
import DBMS.queryProcessing.ExecuteTransactions;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.AbstractJoinAlgorithm;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.BlockNestedLoopJoin;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.HashJoin;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.MergeJoin;
import DBMS.recoveryManager.IRecoveryManager;
import DBMS.recoveryManager.RedoLog;
import DBMS.transactionManager.schedules.AbstractScheduler;
import DBMS.transactionManager.schedules.Protocol2PL;

public abstract class Kernel {

	public static int PORT = 0;
	public static long MEMORY_SIZE_BYTES = 0;
	public static int MEMORY_SIZE_TUPLES = 50;
	
	
	public static boolean IN_RECOVERY_PROCESS = false;
	
	public static boolean ENABLE_RECOVERY = true;
	public static boolean ENABLE_FAST_RECOVERY_STRATEGIE = false;
	public static boolean ENABLE_HOT_COLD_DATA_ALGORITHMS = true;
	public static boolean ENABLE_LOG_REQUESTS = true;
	
	public static String DATABASE_FILES_FOLDER;
		
	public static final String LOG_FILE_NAME = "log.d";

	private static CatalogAccess catalog = new CatalogAccess();
	private static AbstractScheduler scheduler = new Protocol2PL();
	private static ExecuteTransactions executeTransactions = new ExecuteTransactions();
	private static HashMap<String, Class<? extends AbstractJoinAlgorithm>> joinAlgorithms = new HashMap<String, Class<? extends AbstractJoinAlgorithm>>();
	private static Initializer initializer = new Initializer();
	private static SealDBPropertiesManipulation propertiesManipulator;
	private static IRecoveryManager recoveryManager = new RedoLog();
	private static MemoryAcessManager memoryAcessManager = new MemoryAcessManager();
	
	
	public static ExecutorService TRANSACTIONS_EXECUTOR;
	public static int TRANSACTION_NUMBER_OF_WORKERS = 4;
	
	public static final String PROPERTIES_MEMORY_SIZE_BYTES = "memory_size";
	public static final String PROPERTIES_TRASACTION_ID = "last_id_trasaction";
	public static final String PROPERTIES_CONNECTION_ID = "last_id_connection";
	public static final String PROPERTIES_DATABASE_FINALIZE_STATE = "database_finalize_state";
	
	public static final String DATABASE_FINALIZE_STATE_OK = "ok";
	public static final String DATABASE_FINALIZE_STATE_ERROR = "error";
	
	
	public static final char SEQUENTIAL_lOG = '1';
	public static final char FULL_TREE_lOG = '2';
	public static final char HYBRID_TREE_lOG = '3';
	public static final char PARALLEL_HYBRID_TREE_lOG = '4';

	public static char LOG_STRATEGY = SEQUENTIAL_lOG;
	

	public static void start() {
		Kernel.log(Kernel.class, "SEAL-DB Initializing", Level.CONFIG);

		TRANSACTIONS_EXECUTOR = Executors.newFixedThreadPool(TRANSACTION_NUMBER_OF_WORKERS);
		addJoinAlgotithm(BlockNestedLoopJoin.class);
		addJoinAlgotithm(MergeJoin.class);
		addJoinAlgotithm(HashJoin.class);
		
		
		Kernel.log(Kernel.class, "Number of Transactions Works: " + TRANSACTION_NUMBER_OF_WORKERS, Level.CONFIG);
		
		DATABASE_FILES_FOLDER = createDirectory("database");
		loadProperties();
		
		getInitializer().inicializeCatalog();
		
		createFile(DATABASE_FILES_FOLDER + File.separator + LOG_FILE_NAME);
		
		if(ENABLE_RECOVERY)Kernel.getInitializer().loadTableSize();
		
		recoveryManager.start(DATABASE_FILES_FOLDER + File.separator + LOG_FILE_NAME);

		if(ENABLE_HOT_COLD_DATA_ALGORITHMS) {
			memoryAcessManager.start();
			Kernel.log(Kernel.class, "Using " + memoryAcessManager.getAlgorithm().getName() + " Algorithm. Capacity: " + memoryAcessManager.getAlgorithm().getCapacity(), Level.CONFIG);
		}
		
	

	}

	public static void stop() {
		recoveryManager.safeFinalize();
		Kernel.log(Kernel.class, "Safe Finalize...", Level.CONFIG);
	}




	public static void addJoinAlgotithm(Class<? extends AbstractJoinAlgorithm> abstractJoin) {
		joinAlgorithms.put(abstractJoin.getSimpleName(), abstractJoin);
	}

	public static CatalogAccess getCatalog() {
		return catalog;
	}

	public static void setCatalog(CatalogAccess catalog) {
		Kernel.catalog = catalog;
	}

	public static ExecuteTransactions getExecuteTransactions() {
		return executeTransactions;
	}

	public static List<String> getJoinAlgorithmListNames() {
		return new Vector<String>(joinAlgorithms.keySet());
	}

	public static List<String> getSchemasNames() {
		Vector<String> schemas = new Vector<String>(catalog.getSchemaMap().keySet());
		return schemas;
	}

	public static AbstractJoinAlgorithm getJoinAlgorithmIntance(String name) {
		try {
			return joinAlgorithms.get(name).newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			Kernel.exception(Kernel.class, e);
		}
		return null;
	}

	public static AbstractScheduler getScheduler() {
		return scheduler;
	}

	public static void setScheduler(AbstractScheduler scheduler) {
		Kernel.scheduler = scheduler;
	}


	public static IRecoveryManager getRecoveryManager() {
		return recoveryManager;
	}

	public static Initializer getInitializer() {
		return initializer;
	}

	public static void log(Class<?> c, Object msg, Level level) {
		System.out.println("[" + new Date().toString() + ":" + c.getSimpleName() + "] " + msg.toString());

	}

	public static void exception(Class<?> c, Exception e) {
		System.out.println("ERR0 [" + new Date().toString() + ":" + c.getSimpleName() + "] " + e.getMessage());
		e.printStackTrace();

	}

	public static String createDirectory(String name) {
		try {
			File diretorio = new File(name);
			if (!diretorio.exists()) {
				Files.createDirectory(Paths.get(System.getProperty("user.dir") + File.separator + name));
			} else {
				// Kernel.info(Kernel.class,name+" directory found
				// successfully...",Level.CONFIG);
			}
		} catch (IOException e) {
			Kernel.exception(Kernel.class, e);
		}
		return name;
	}

	public static void removeFile(String path) {
		try {
			File f = new File(path);
			f.delete();
		} catch (Exception e) {
			Kernel.exception(Kernel.class, e);
		}
	}

	
	public static File createFile(String path) {
		try {
			File file = new File(path);
			file.createNewFile();
			return file;
		} catch (IOException e) {
			Kernel.exception(Kernel.class, e);
		}
		return null;
	}
	
	
	private static void loadProperties(){
		String CONFIG_FILE = DATABASE_FILES_FOLDER + File.separator + "configs.properties";
		
		Properties properties = new Properties();
		try {
			//Kernel.info(Kernel.class,"Config Path: "+path,Level.CONFIG);
			Kernel.log(Kernel.class,"Config File: "+CONFIG_FILE,Level.CONFIG);
			File file = new File(CONFIG_FILE);
			file.createNewFile();
			
			propertiesManipulator = new SealDBPropertiesManipulation();
			propertiesManipulator.file = file;
			propertiesManipulator.properties = properties;
			
			checkProperties(PROPERTIES_MEMORY_SIZE_BYTES, "1074000000");
			checkProperties(PROPERTIES_TRASACTION_ID, "1");
			checkProperties(PROPERTIES_CONNECTION_ID, "1");
			checkProperties(PROPERTIES_DATABASE_FINALIZE_STATE, DATABASE_FINALIZE_STATE_ERROR);
			
			
			if(MEMORY_SIZE_BYTES <=0 )MEMORY_SIZE_BYTES = Long.parseLong(getConfig(PROPERTIES_MEMORY_SIZE_BYTES));
			
		} catch (IOException e) {
			Kernel.exception(Kernel.class,e);
		}
		
	}
	private static void checkProperties(String type,String defaultValue) throws IOException{
		propertiesManipulator.load();
		String value = propertiesManipulator.properties.getProperty(type);
		if(value==null){
			propertiesManipulator.properties.put(type,defaultValue);
			propertiesManipulator.store();
		}
	}
	

	static class SealDBPropertiesManipulation {
		File file;
		Properties properties;
		FileInputStream inputStream;
		FileOutputStream outputStream;

		void load() throws IOException {
			inputStream = new FileInputStream(file);
			properties.load(inputStream);
			inputStream.close();

		}

		void store() throws IOException {
			outputStream = new FileOutputStream(file);
			properties.store(outputStream, null);
			outputStream.close();
		}
	}
	
	public synchronized static int getNewID(String type) {

		try {
			propertiesManipulator.load();
			int value = Integer.parseInt(propertiesManipulator.properties.getProperty(type));
			value++;
			propertiesManipulator.properties.replace(type, value + "");
			propertiesManipulator.store();
			return value;
		} catch (Exception e) {
			Kernel.exception(Kernel.class, e);
		}
		return 0;
	}

	public synchronized static void setFinalizeStateDatabase(String state) {
		try {
			propertiesManipulator.load();
			propertiesManipulator.properties.replace(PROPERTIES_DATABASE_FINALIZE_STATE, state);
			propertiesManipulator.store();
		} catch (Exception e) {
			Kernel.exception(Kernel.class, e);
		}
	}

	public static String getFinalizeStateDatabase() {
		try {
			propertiesManipulator.load();
			return propertiesManipulator.properties.getProperty(PROPERTIES_DATABASE_FINALIZE_STATE);
		} catch (Exception e) {
			Kernel.exception(Kernel.class, e);
		}
		return null;
	}

	public static String getConfig(String propertie) {
		try {
			propertiesManipulator.load();
			return propertiesManipulator.properties.getProperty(propertie);
		} catch (Exception e) {
			Kernel.exception(Kernel.class, e);
		}
		return null;
	}

	public static MemoryAcessManager getMemoryAcessManager() {
		return memoryAcessManager;
	}
	

}
