package DBMS;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import DBMS.bufferManager.IBufferManager;
import DBMS.bufferManager.policies.AbstractBufferPolicy;
import DBMS.bufferManager.policies.FIFO;
import DBMS.bufferManager.policies.LRU;
import DBMS.bufferManager.policies.MRU;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.fileManager.ISchema;
import DBMS.fileManager.WriterProcess;
import DBMS.fileManager.catalog.CatalogInitializer;
import DBMS.fileManager.catalog.ICatalog;
import DBMS.queryProcessing.ExecuteTransactions;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.AbstractJoinAlgorithm;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.BlockNestedLoopJoin;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.HashJoin;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.MergeJoin;
import DBMS.recoveryManager.IRecoveryManager;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionManagerListener;
import DBMS.transactionManager.TransactionRunnable;
import DBMS.transactionManager.schedules.AbstractScheduler;
import DBMS.transactionManager.schedules.Protocol2PL;


public abstract class Kernel {
	
	
	/**
	 * Number of pages
	 */
	public static int BUFFER_SIZE = 0;
	public static int BUFFER_SIZE_TEMP = 0;
	public static int BLOCK_SIZE = 0;
	public static int PORT = 0;
	
	public static boolean ENABLE_RECOVERY = true;
	public static boolean ENABLE_TEMP_BUFFER = false;
	public static boolean ENABLE_LOAD_ALL_PAGES_IN_MEMORY_MODE = false;
	public static boolean ENABLE_LOG_BUFFER_REQUESTS = false;
	public static boolean ENABLE_SPECIFIC_LOG_PATH = false;
	
	public static int CHECKPOINT_INTERVAL = 20;
	
	public static String DATABASE_FILES_FOLDER;
	public static String SCHEMAS_FOLDER;
	public static String DEFAULT_ROOT_SCHEMA_FOLDER;
	public static String SYSTEM_EVENTS_LOG_FOLDER;
	
	private static final Logger EVENTS_LOGGER = Logger.getLogger("events_log");  	
	public static final String DEFAULT_ROOT_SCHEMA_NAME = "seal-db";
	public static final String CONFIG_FOLDER_NAME = "configs";
	public static final String LOG_FOLDER_NAME = "log";
	public static final String BACKUP_FOLDER_NAME = "backup";
	

	public static String DATABASE_FILES_FOLDER_NAME = "database";
	public static String SCHEMAS_FOLDER_NAME = "schemas";
	
	
	public static String LOG_FILE_NAME = "log.b";
	private static String LOG_FILE = null;
	private static String LOG_FOLDER_PATH = null;
	private static String CONFIG_FILE = null;
	public static String BACKUP_FOLDER_PATH = null;
	public static String CONFIG_FOLDER_PATH = null;
	
	
	private static WriterProcess writerProcess = new WriterProcess();;
	private static ICatalog catalog = ICatalog.getInstance();
	private static IBufferManager bufferManager = IBufferManager.getInstance();
	private static AbstractScheduler scheduler = new Protocol2PL();
	private static ExecuteTransactions executeTransactions = new ExecuteTransactions();
	private static TransactionManagerListener TransactionManagerListener;
	private static HashMap<String, Class<? extends AbstractJoinAlgorithm>> joinAlgorithms = new HashMap<String, Class<? extends AbstractJoinAlgorithm>>();
	private static HashMap<String, Class<? extends AbstractBufferPolicy>> bufferPolicies = initializeMapDefaultBufferPolicies();
	private static IRecoveryManager recoveryManager;
	private static CatalogInitializer catalagInitializer = new CatalogInitializer();
	private static SealDBPropertiesManipulation propertiesManipulator;
	
	
	public static final String PROPERTIES_PORT = "port";
	
	public static final String PROPERTIES_BLOCK_SIZE = "block_size";
	public static final String PROPERTIES_BUFFER_SIZE = "buffer_size";
	public static final String PROPERTIES_BUFFER_SIZE_TEMP = "buffer_size_temp";
	public static final String PROPERTIES_BUFFER_COLD_SIZE = "buffer_cold_size";
	
	public static final String PROPERTIES_LOG_PATH = "log_path";
	public static final String PROPERTIES_LOG_FILE_NAME = "log_file_name";
	
	public static final String PROPERTIES_TRASACTION_ID = "last_id_trasaction";
	public static final String PROPERTIES_CONNECTION_ID = "last_id_connection";
	public static final String PROPERTIES_DATABASE_FINALIZE_STATE = "database_finalize_state";
	
	public static final String DATABASE_FINALIZE_STATE_OK = "ok";
	public static final String DATABASE_FINALIZE_STATE_ERROR = "error";


	private static void loadProperties(){
		CONFIG_FOLDER_PATH = createDirectory( Kernel.DATABASE_FILES_FOLDER + File.separator+CONFIG_FOLDER_NAME) ;
		CONFIG_FILE = CONFIG_FOLDER_PATH+File.separator+"seal-db-general-configs.properties";
		Properties properties = new Properties();
		try {
			//Kernel.info(Kernel.class,"Config Path: "+path,Level.CONFIG);
			Kernel.log(Kernel.class,"Config File: "+CONFIG_FILE,Level.CONFIG);
			File file = new File(CONFIG_FILE);
			file.createNewFile();
			
			propertiesManipulator = new SealDBPropertiesManipulation();
			propertiesManipulator.file = file;
			propertiesManipulator.properties = properties;
			
			
			checkProperties(PROPERTIES_LOG_FILE_NAME, LOG_FILE_NAME);
			checkProperties(PROPERTIES_PORT, "3000");
			checkProperties(PROPERTIES_BLOCK_SIZE, "4096");
			checkProperties(PROPERTIES_BUFFER_SIZE, "39");
			checkProperties(PROPERTIES_BUFFER_SIZE_TEMP, "10");
			
			checkProperties(PROPERTIES_TRASACTION_ID, "1");
			checkProperties(PROPERTIES_CONNECTION_ID, "1");
			checkProperties(PROPERTIES_DATABASE_FINALIZE_STATE, DATABASE_FINALIZE_STATE_ERROR);
			
		
			if(LOG_FILE_NAME == null || LOG_FILE_NAME.isEmpty())LOG_FILE_NAME = getConfig(PROPERTIES_LOG_FILE_NAME);
			if(PORT <= 0) PORT = Integer.parseInt(getConfig(PROPERTIES_PORT));
			if(BLOCK_SIZE <= 0)BLOCK_SIZE = Integer.parseInt(getConfig(PROPERTIES_BLOCK_SIZE));
			if(BUFFER_SIZE <=0 )BUFFER_SIZE = Integer.parseInt(getConfig(PROPERTIES_BUFFER_SIZE));
			if(BUFFER_SIZE_TEMP <=0 )BUFFER_SIZE_TEMP = Integer.parseInt(getConfig(PROPERTIES_BUFFER_SIZE_TEMP));	
			
			if(ENABLE_SPECIFIC_LOG_PATH) {
				checkProperties(PROPERTIES_LOG_PATH, Kernel.SCHEMAS_FOLDER + File.separator+DEFAULT_ROOT_SCHEMA_NAME + File.separator + LOG_FOLDER_NAME);
				LOG_FOLDER_PATH = getConfig(PROPERTIES_LOG_PATH);
			}else {
				LOG_FOLDER_PATH = Kernel.SCHEMAS_FOLDER + File.separator+DEFAULT_ROOT_SCHEMA_NAME + File.separator + LOG_FOLDER_NAME;
			}
			LOG_FILE = LOG_FOLDER_PATH+File.separator+LOG_FILE_NAME;
		
		} catch (IOException e) {
			Kernel.exception(Kernel.class,e);
		}
		
	}
	
	
	
	private static void createBackupFolder(){
		BACKUP_FOLDER_PATH = createDirectory( Kernel.SCHEMAS_FOLDER + File.separator+DEFAULT_ROOT_SCHEMA_NAME + File.separator+BACKUP_FOLDER_NAME);
	}
	
	public static File createFile(String path){
		try {
			File file = new File(path);
			file.createNewFile();
			return file;
		} catch (IOException e) {
			Kernel.exception(Kernel.class,e);
		}
		return null;
	}
	
	public static void loadRecovery(){
		createDirectory( LOG_FOLDER_PATH);
		try {
			File file = new File(LOG_FILE);
			file.createNewFile();
		} catch (IOException e) {
			Kernel.exception(Kernel.class,e);
		}
		recoveryManager = IRecoveryManager.getInstance();
		recoveryManager.start(LOG_FILE);		
	}
	
	
	
	

	private static void checkProperties(String type,String defaultValue) throws IOException{
		propertiesManipulator.load();
		String value = propertiesManipulator.properties.getProperty(type);
		if(value==null){
			propertiesManipulator.properties.put(type,defaultValue);
			propertiesManipulator.store();
		}
	}
	
	
	
	public static void start(){

		DATABASE_FILES_FOLDER = createDirectory(DATABASE_FILES_FOLDER_NAME);
		SCHEMAS_FOLDER = createDirectory(DATABASE_FILES_FOLDER_NAME+File.separator+SCHEMAS_FOLDER_NAME);
		DEFAULT_ROOT_SCHEMA_FOLDER = Kernel.createDirectory(Kernel.SCHEMAS_FOLDER + File.separator+ DEFAULT_ROOT_SCHEMA_NAME);
		
		SYSTEM_EVENTS_LOG_FOLDER = Kernel.createDirectory(Kernel.SCHEMAS_FOLDER + File.separator+ DEFAULT_ROOT_SCHEMA_NAME + File.separator + "events_log");
		
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
		Date current = new Date();
		
		try {
			
			FileHandler fileText =  new FileHandler(SYSTEM_EVENTS_LOG_FOLDER + File.separator + "events_"+dt.format(current)+".log", true);
			FileHandler fileHTML =  new FileHandler(SYSTEM_EVENTS_LOG_FOLDER + File.separator + "events_"+dt.format(current)+".html", true);

	        // create a TXT formatter
			SimpleFormatter formatterText = new SimpleFormatter();
	        fileText.setFormatter(formatterText);
	        EVENTS_LOGGER.addHandler(fileText);

	        // create an HTML formatter
	        MyHtmlFormatter formatterHTML = new MyHtmlFormatter();
	        fileHTML.setFormatter(formatterHTML);
	        EVENTS_LOGGER.addHandler(fileHTML);
	        
	       Logger root = Logger.getLogger("");
	        root.setLevel(Level.CONFIG);
	        for (Handler handler : root.getHandlers()) {
	            if (handler instanceof ConsoleHandler) {
	            	System.out.println(handler.getClass().getName());
	                // java.util.logging.ConsoleHandler.level = ALL
	                handler.setLevel(Level.CONFIG);
	            }
	        }
			
	
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Kernel.log(Kernel.class, "SEAL-DB Initializing",Level.CONFIG);
		loadProperties();
		
		if(getBufferManager().getBufferPolicy()==null){
			setBufferPolicy(LRU.class);		
		}else{
			setBufferPolicy(getBufferManager().getBufferPolicy().getClass());
		}
		
		
		Kernel.log(Kernel.class,"Using "+getBufferManager().getBufferPolicy().getClass().getSimpleName()+" Buffer Policy",Level.CONFIG);
		
		getCatalagInitializer().inicializeCatalog();
		loadRecovery();
		createBackupFolder();
		removeTemporaryFiles();
		addJoinAlgotithm(BlockNestedLoopJoin.class);
		addJoinAlgotithm(MergeJoin.class);
		addJoinAlgotithm(HashJoin.class);		
		getTransactionManager().startPersistenceMessageService();	
		
		if(!ENABLE_LOAD_ALL_PAGES_IN_MEMORY_MODE) {
			getBufferManager().freeAll();
			getBufferManager().resetStatistics();		
			Kernel.getWriterProcess().start();
		}
		
		getBufferManager().startTempBufferPolicy();
		
		
		if(ENABLE_LOAD_ALL_PAGES_IN_MEMORY_MODE)loadAllPagesInMemory();
		
	}
	public static void stop(){
		removeTemporaryFiles();
		getTransactionManager().getPersistenceMessageService().commit();
		recoveryManager.safeFinalize();
		Kernel.getWriterProcess().stop();
		Kernel.log(Kernel.class,"Safe Finalize...",Level.CONFIG);
		for(Handler h : EVENTS_LOGGER.getHandlers()){
		    h.close();  
		}
		
	}
	private static HashMap<String, Class<? extends AbstractBufferPolicy>> initializeMapDefaultBufferPolicies(){
		HashMap<String, Class<? extends AbstractBufferPolicy>> bufferPolicies =  new HashMap<String, Class<? extends AbstractBufferPolicy>>();
		bufferPolicies.put(LRU.class.getSimpleName(), LRU.class);
		bufferPolicies.put(MRU.class.getSimpleName(), MRU.class);
		bufferPolicies.put(FIFO.class.getSimpleName(), FIFO.class);
		return bufferPolicies;
	}
	
	
	public static String createDirectory(String name){
		try
        {
			File diretorio = new File(name); 
			if (!diretorio.exists()) {  
			   Files.createDirectory( Paths.get(System.getProperty("user.dir") + File.separator+name));
			} else {  
			 //  Kernel.info(Kernel.class,name+" directory found successfully...",Level.CONFIG);
			}  
        } catch (IOException e)
        {
        	Kernel.exception(Kernel.class,e);
        }
		return name;
	}
	
	
	
	public static void removeTemporaryFiles() {
		
		for (ISchema schema : catalog.getShemas()) {
			File f = new File(schema.getTempFolder());
			if (f.isDirectory()) {
				File[] files = f.listFiles();
				
				for (File file : files) {
					file.delete();
				}
			}			
		}
		
	}
	
	public static void removeFile(String path){
		try{
			File f = new File(path);
			f.delete();	
		}catch (Exception e) {
			Kernel.exception(Kernel.class,e);
		}
	}
	
	
	

	static class SealDBPropertiesManipulation{
		File file;
		Properties properties;
		FileInputStream inputStream ;
		FileOutputStream outputStream;
		
		void load() throws IOException{
			inputStream = new FileInputStream(file);
			properties.load( inputStream );
			inputStream.close();
			
		}
		
		void store() throws IOException{
			outputStream = new FileOutputStream(file);
			properties.store(outputStream,null);
			outputStream.close();
		}
	}
	
	public synchronized static int getNewID(String type){
		
		try{
			propertiesManipulator.load();
			int value = Integer.parseInt(propertiesManipulator.properties.getProperty(type));
			value++;
			propertiesManipulator.properties.replace(type, value+"");
			propertiesManipulator.store();
			return value;
		}catch (Exception e) {
			Kernel.exception(Kernel.class,e);
		}
		return 0;
	}
	
	public synchronized static void setFinalizeStateDatabase(String state){
		try{
			propertiesManipulator.load();
			propertiesManipulator.properties.replace(PROPERTIES_DATABASE_FINALIZE_STATE, state);
			propertiesManipulator.store();
		}catch (Exception e) {
			Kernel.exception(Kernel.class,e);
		}
	}
	
	public static String getFinalizeStateDatabase(){
		try{
			propertiesManipulator.load();
			return propertiesManipulator.properties.getProperty(PROPERTIES_DATABASE_FINALIZE_STATE);
		}catch (Exception e) {
			Kernel.exception(Kernel.class,e);
		}
		return null;
	}
	
	public static String getConfig(String propertie){
		try{
			propertiesManipulator.load();
			return propertiesManipulator.properties.getProperty(propertie);
		}catch (Exception e) {
			Kernel.exception(Kernel.class,e);
		}
		return null;
	}
	
	
	public static void addJoinAlgotithm(Class<? extends AbstractJoinAlgorithm> abstractJoin){
		joinAlgorithms.put(abstractJoin.getSimpleName(), abstractJoin);
	}
	public static void addBufferPolicy(Class<? extends AbstractBufferPolicy> abstractBufferPolicy){
		bufferPolicies.put(abstractBufferPolicy.getSimpleName(), abstractBufferPolicy);
	}
	
	public static ICatalog getCatalog() {
		return catalog;
	}
	public static void setCatalog(ICatalog catalog) {
		Kernel.catalog = catalog;
	}
	public static IBufferManager getBufferManager() {
		return bufferManager;
	}
	public static void setBufferManager(IBufferManager bufferManager) {
		Kernel.bufferManager = bufferManager;
	}
	public static ExecuteTransactions getExecuteTransactions() {
		return executeTransactions;
	}
	
	public static List<String> getJoinAlgorithmListNames(){
		return new Vector<String>(joinAlgorithms.keySet());
	}
	public static List<String> getBufferPoliciesListNames(){
		return new Vector<String>(bufferPolicies.keySet());
	}
	public static List<String> getSchemasNames(){
		Vector<String> schemas = new Vector<String>(catalog.getSchemaMap().keySet());
		Collections.reverse(schemas);
		return schemas;
	}
	
	
	public static void setBufferPolicy(String name ){

		setBufferPolicy(bufferPolicies.get(name));
	}
	
	public static void setBufferPolicy(Class<? extends AbstractBufferPolicy> bufferPolicy){
	
		try {	
			int capacity = Kernel.BUFFER_SIZE;
			int coldPercet = 90;
			if(ENABLE_LOAD_ALL_PAGES_IN_MEMORY_MODE && Kernel.BUFFER_SIZE > 0) {
				capacity = (Kernel.BUFFER_SIZE * coldPercet) / 100;
				Kernel.log(Kernel.class,"In-Memory Mode [Buffer Size: " + Kernel.BUFFER_SIZE + "], [Temp Size: " + Kernel.BUFFER_SIZE_TEMP + "], [Initial Size("+coldPercet+"%): "+capacity+"]",Level.CONFIG);
			}
			
			bufferManager.setBufferPolicy(bufferPolicy.getDeclaredConstructor(Integer.class).newInstance(new Integer(capacity)));

		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | SecurityException | InvocationTargetException | NoSuchMethodException e) {
			Kernel.exception(Kernel.class,e);
		}

	}
	
	public static AbstractJoinAlgorithm getJoinAlgorithmIntance(String name){
		try {
			return joinAlgorithms.get(name).newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			Kernel.exception(Kernel.class,e);
		}
		return null;
	}
	public static AbstractScheduler getScheduler() {
		return scheduler;
	}
	public static void setScheduler(AbstractScheduler scheduler) {
		Kernel.scheduler = scheduler;
	}
	public static TransactionManagerListener getTransactionManagerListener() {
		return TransactionManagerListener;
	}
	public static void setTransactionManagerListener(TransactionManagerListener transactionManagerListener) {
		Kernel.TransactionManagerListener = transactionManagerListener;
	}

	public static DistributedTransactionManagerController getTransactionManager(){
		return DistributedTransactionManagerController.getInstance(); 
	}
	public static IRecoveryManager getRecoveryManager() {
		return recoveryManager;
	}
	
	public static WriterProcess getWriterProcess() {
		return writerProcess;
	}


	public static CatalogInitializer getCatalagInitializer() {
		return catalagInitializer;
	}
	
	

	public static void log(Class<?> c, Object msg, Level level){
		//level = Level.CONFIG;
	//	System.out.println("["+new Date().toString()+":"+c.getSimpleName()+"] " + msg.toString() );
		EVENTS_LOGGER.log(level, ("["+c.getSimpleName()+"] " + msg.toString()));
	}
	
	public static void exception(Class<?> c, Exception e){
		//e.printStackTrace();
		//System.out.println("["+new Date().toString()+":"+c.getSimpleName()+"] " + e.getMessage() );
		EVENTS_LOGGER.severe("["+c.getSimpleName()+"] " +getStackTrace(e));
	
	}
	private static String getStackTrace(Exception ex) {
	    StringBuffer sb = new StringBuffer(500);
	    StackTraceElement[] st = ex.getStackTrace();
	    sb.append(ex.getClass().getName() + ": " + ex.getMessage() + "\n");
	    for (int i = 0; i < st.length; i++) {
	      sb.append("\t at " + st[i].toString() + "\n");
	    }
	    return sb.toString();
	}
	

	static class MyHtmlFormatter extends Formatter {

	    public String format(LogRecord rec) {
	        StringBuffer buf = new StringBuffer(1000);
	        buf.append("<tr>\n");

	        if(rec.getLevel() == Level.CONFIG){
	        	buf.append("\t<td style=\"color:#006400\">");
	            buf.append("<b>");
	            buf.append(rec.getLevel());
	            buf.append("</b>");
	            
	        }else if(rec.getLevel() == Level.WARNING){
	        	buf.append("\t<td style=\"color:#FFAC00\">");
	            buf.append("<b>");
	            buf.append(rec.getLevel());
	            buf.append("</b>");
	        }else if (rec.getLevel().intValue() >= Level.WARNING.intValue()) {
	            buf.append("\t<td style=\"color:red\">");
	            buf.append("<b>");
	            buf.append(rec.getLevel());
	            buf.append("</b>");
	        } else {
	            buf.append("\t<td>");
	            buf.append(rec.getLevel());
	        }

	        buf.append("</td>\n");
	        buf.append("\t<td>");
	        buf.append(calcDate(rec.getMillis()));
	        buf.append("</td>\n");
	        buf.append("\t<td>");
	        buf.append(formatMessage(rec));
	        buf.append("</td>\n");
	        buf.append("</tr>\n");

	        return buf.toString();
	    }

	    private String calcDate(long millisecs) {
	        SimpleDateFormat date_format = new SimpleDateFormat("MMM dd,yyyy HH:mm");
	        Date resultdate = new Date(millisecs);
	        return date_format.format(resultdate);
	    }

	    // this method is called just after the handler using this
	    // formatter is created
	    public String getHead(Handler h) {
	        return "<!DOCTYPE html>\n<head>\n<style>\n"
	            + "table { width: 100% }\n"
	            + "th { font:bold 13pt Tahoma; }\n"
	            + "td { font:normal 10pt Tahoma; }\n"
	            + "h1 {font:normal 13pt Tahoma;}\n"
	            + "</style>\n"
	            + "</head>\n"
	            + "<body>\n"
	            + "<h1>" + (new Date()) + "</h1>\n"
	            + "<table border=\"0\" cellpadding=\"5\" cellspacing=\"3\">\n"
	            + "<tr align=\"left\">\n"
	            + "\t<th style=\"width:10%\">Log level</th>\n"
	            + "\t<th style=\"width:15%\">Time</th>\n"
	            + "\t<th style=\"width:75%\">Log Message</th>\n"
	            + "</tr>\n";
	      }

	    // this method is called just after the handler using this
	    // formatter is closed
	    public String getTail(Handler h) {
	        return "</table>\n</body>\n</html>";
	    }
	}

	public static int INITIAL_NUMBER_OF_PAGES = 0;
	public static void loadAllPagesInMemory() {
//		new Thread(new Runnable() {
//			@Override
//			public void run() {
//				
				Kernel.log(Kernel.class, "Loading the data into memory...",Level.CONFIG);
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e2) {
					e2.printStackTrace();
				}
				
				bufferManager.setEnableColdDataLoad(true);
				
				
				for (ISchema schema : Kernel.getCatalog().getShemas()) {

					DBConnection tempConnection = Kernel.getTransactionManager().getConnectionService().getSystemConnection(schema.getName());
					ITransaction transaction = Kernel.getExecuteTransactions().begin(tempConnection, false, false);

					for (ITable table : schema.getTables()) {

						if (!table.isTemp()) {

							try {
								final CountDownLatch latch = new CountDownLatch(1);

								transaction.execRunnable(new TransactionRunnable() {

									@Override
									public void run(ITransaction transaction) {

										TableScan tableScan = new TableScan(transaction, table);
										tableScan.reset();

										while ((tableScan.nextBlock()) != null) {
											INITIAL_NUMBER_OF_PAGES++;
										}

										latch.countDown();
									}

									@Override
									public void onFail(ITransaction transaction, Exception e) {
										latch.countDown();

									}
								});

								latch.await();
							} catch (InterruptedException e1) {
								Kernel.exception(Kernel.class, e1);
							}
						}
					}
				}
				Kernel.log(Kernel.class, INITIAL_NUMBER_OF_PAGES+" loaded in-Memory",Level.CONFIG);
				bufferManager.setEnableColdDataLoad(false);
			//}
			
//		}).start();


	}

}
