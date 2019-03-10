package DBMS.distributed.resourceManager;


import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.connectionManager.Dispatcher;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;

public class PersistenceMessageService {

	private final String MESSAGE_LOG_NAME = "message_log";
	private final String TRANSACTION_LOG = "termination_protocol_log";
	
	private Dispatcher dispatcher;
	
	private DBConnection logConnection;
	
	private ITransaction logTransaction;

	private Parse parse;
	private ISchema schemaManipulate;

	public PersistenceMessageService(Dispatcher dispatcher) {
		this.setDispatcher(dispatcher);
		parse = new Parse();
		logConnection = dispatcher.getServer().getLocalConnection(Kernel.DEFAULT_ROOT_SCHEMA_NAME, "admin", "admin");
		logTransaction = Kernel.getExecuteTransactions().begin(logConnection,false,false);
		schemaManipulate = Kernel.getCatalog().getSchemabyName(Kernel.DEFAULT_ROOT_SCHEMA_NAME);		
		try {
			createTables();
		} catch (SQLException e) {
			//e.printStackTrace();
		}
		
	}
	
	private void createTables() throws SQLException{	
		
		//String sql= "Create table "+ MESSAGE_LOG_NAME+" (leader_ip varchar,  participant_ip varchar,  transaction_id int,  status varchar , messsage varchar)";
		
		//exeSQL(sql);
		
		//sql = "Create table "+ TRANSACTION_LOG+" (leader_ip varchar,  participant_ip varchar,  transaction_id int)";
		
		//exeSQL(sql);
	}
	
	public void insertIntoMessageLog(String leaderIp,String participantIp, int trasactionId, String status, String message){
		try {
			String sql = "INSERT INTO "+MESSAGE_LOG_NAME+" ( leader_ip,  participant_ip,  transaction_id,  status, messsage) "
					+ "VALUES ("+leaderIp + "," +participantIp+"," +trasactionId+","+status+","+message+")";
			//Kernel.info(this.getClass(),sql);
			exeSQL(sql);
			Kernel.getBufferManager().flush();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void insertIntoTransactionLog(String leaderIp,String participantIp, int trasactionId){
		try {
			String sql = "INSERT INTO "+TRANSACTION_LOG+" ( leader_ip,  participant_ip,  transaction_id) "
					+ "VALUES ("+leaderIp + "," + participantIp + "," +trasactionId+")";
			exeSQL(sql);
			//Kernel.info(this.getClass(),sql);
			Kernel.getBufferManager().flush();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public List<ITuple> selectMessageLog(String where){ // leader_ip = 192.1.2.3 and  transaction_id = 5
	
		return where.isEmpty() ? execWithResult("select * from " + MESSAGE_LOG_NAME) : 
			execWithResult("select * from " + MESSAGE_LOG_NAME + " where " + where);
	}
	public List<ITuple> selectTransactionLog(String where){
		return where.isEmpty() ? execWithResult("select * from " + TRANSACTION_LOG) : 
			execWithResult("select * from " + TRANSACTION_LOG + " where " + where);
	}
	
	
	private List<ITuple> execWithResult(String sql){ 
		ITable table = null;
		try {
			table = exeSQLWithTable(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if (table != null) {
			LinkedList<ITuple> tuples = new LinkedList<>();
			TableScan scan = new TableScan(logTransaction, table);
			ITuple tuple = null;
			while ((tuple = scan.nextTuple()) != null) {
				tuples.add(tuple);
				
				
			}
			return !tuples.isEmpty() ? tuples : null;
		}
		return null;
	}
	
	private void exeSQL(String sql) throws SQLException{
		//LogError.save(this.getClass(),sql);
		/*Desligado
		List<Plan> p = parse.parse(sql, schemaManipulate);
		for (Plan plan : p) {
			plan.setTransaction(logTransaction);
			plan.execute();
		}
		*/
		System.out.println("Try exec SQL > PersistenceMessageService ");
	}
	
	private ITable exeSQLWithTable(String sql) throws SQLException{
		//LogError.save(this.getClass(),sql);
		List<Plan> p = parse.parse(sql, schemaManipulate);
		for (Plan plan : p) {
			plan.setTransaction(logTransaction);
			return plan.execute();
		}
		return null;
	}


	public Dispatcher getDispatcher() {
		return dispatcher;
	}


	public void setDispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}
	
	public void commit(){
		logTransaction.commit();
	}
	
}
