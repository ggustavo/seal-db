package DBMS.distributed;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.connectionManager.ResponseMenssageListener;
import DBMS.distributed.commitprotocols.TwoPhaseProtocol;
import DBMS.distributed.resourceManager.message.MessageHeader;
import DBMS.distributed.resourceManager.message.types.QueryMenssage;
import DBMS.distributed.resourceManager.message.types.ResultMenssage;
import DBMS.fileManager.Column;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.TableManipulate;
import DBMS.transactionManager.ITransaction;

public class Branch {
	private ResourceManagerConnection connection;
	private boolean isCommit = false;
	private String response;
	private String id;
	private String transactionGlobalID;
	private int lastRemoteResult;
	private ITransaction localTransaction;

	public Branch(ResourceManagerConnection remoteDBConnection) {
		this.connection = remoteDBConnection;
		lastRemoteResult = 0;
		localTransaction = Kernel.getExecuteTransactions().begin(Kernel.getTransactionManager().getLocalConnection("seal-db", "admin", "admin"));
	}

	public void begin() {

		try {
			final CountDownLatch latch = new CountDownLatch(1);

			String idConnection = String.valueOf(connection.getDbConnection().getId());

			connection.getTransactionManager().getDispatcher().sendResquest(
					connection.getAddress(),
					connection.getPort(),
					idConnection, 
					MessageHeader.BEGIN_TRANSACTION,
					new ResponseMenssageListener<String>() {

						@Override
						public void onReceiver(String o) {
							id = o;
							latch.countDown();
						}

						@Override
						public void onErro(String e) {
							Kernel.log(this.getClass(),"Begin Error: "+e,Level.SEVERE);
							latch.countDown();
						}

						@Override
						public Class<String> responseDataClass() {
							return String.class;
						}
					}, DistributedTransactionManagerController.TRANSACTION_MANAGER_TIMEOUT);

			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} catch (SocketTimeoutException e) {
			e.printStackTrace();
		}
	}
	
	public String prepare() {
		if(id==null || id.isEmpty()){
			Kernel.log(this.getClass(),"No open transaction  for prepare",Level.WARNING);
		}else{
			
			try {
				final CountDownLatch latch = new CountDownLatch(1);

				Map<String, String> mapIDs = new HashMap<>();
				mapIDs.put("idL", id);
				mapIDs.put("idG", transactionGlobalID);
				
				connection.getTransactionManager().getDispatcher().sendResquest(
						connection.getAddress(),
						connection.getPort(),
						mapIDs, 
						MessageHeader.PREPARE,
						new ResponseMenssageListener<String>() {

							@Override
							public void onReceiver(String o) {
								response = o;
								latch.countDown();
							}

							@Override
							public void onErro(String e) {
								response = e;
								latch.countDown();
							}

							@Override
							public Class<String> responseDataClass() {
								return String.class;
							}
						}, TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);

				latch.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (SocketTimeoutException e) {
					response = "timeout";
					Kernel.log(this.getClass(),"Prepare msg timeout",Level.SEVERE);
			}
		}
		return response;
	}
	
	public String commit(){
		
		if(id==null || id.isEmpty()){
			Kernel.log(this.getClass(),"No open transaction  for commit",Level.WARNING);
		}else if(isCommit==false){
			
			localTransaction.commit();
			
			try {
				final CountDownLatch latch = new CountDownLatch(1);

				Map<String, String> mapIDs = new HashMap<>();
				mapIDs.put("idL", id);
				mapIDs.put("idG", transactionGlobalID);

				connection.getTransactionManager().getDispatcher().sendResquest(
						connection.getAddress(),
						connection.getPort(),
						mapIDs, 
						MessageHeader.COMMIT_TRANSACTION,
						new ResponseMenssageListener<String>() {

							@Override
							public void onReceiver(String o) {
								response = o;
								latch.countDown();
								isCommit = true;
							}

							@Override
							public void onErro(String e) {
								response = e;
								Kernel.log(this.getClass(),e,Level.SEVERE);
								latch.countDown();
							}

							@Override
							public Class<String> responseDataClass() {
								return String.class;
							}
						}, TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);

				latch.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (SocketTimeoutException e) {
				response = "timeout";
				Kernel.log(this.getClass(),"TIMEOUT",Level.SEVERE);
			}
		}
		return response;
	}
	
	
	public boolean abort(){
		if(id==null || id.isEmpty()){
			Kernel.log(this.getClass(),"No Transaction open",Level.WARNING);
			return false;
		}else if(isCommit==false){
			
			localTransaction.abort();
			
			try {
				final CountDownLatch latch = new CountDownLatch(1);

				Map<String, String> mapIDs = new HashMap<>();
				mapIDs.put("idL", id);
				mapIDs.put("idG", transactionGlobalID);

				connection.getTransactionManager().getDispatcher().sendResquest(
						connection.getAddress(),
						connection.getPort(),
						mapIDs, 
						MessageHeader.ABORT,
						new ResponseMenssageListener<String>() {

							@Override
							public void onReceiver(String o) {
								
								latch.countDown();
								isCommit = false;
							}

							@Override
							public void onErro(String e) {
								Kernel.log(this.getClass(),e,Level.SEVERE);
								latch.countDown();
							}

							@Override
							public Class<String> responseDataClass() {
								return String.class;
							}
						}, TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);

				latch.await();
			} catch (InterruptedException e1) {
			} catch (SocketTimeoutException e) {
				isCommit = false;
			}
		}
		return isCommit;
	}
	
	
	public String status(){
		final String[] response = new String[1];

		if(id==null || id.isEmpty()){
			Kernel.log(this.getClass(),"No Transaction open",Level.WARNING);
			return null;
		}else if(isCommit==false){
			try {
				final CountDownLatch latch = new CountDownLatch(1);

				String idTransaction = id;

				connection.getTransactionManager().getDispatcher().sendResquest(
						connection.getAddress(),
						connection.getPort(),
						idTransaction, 
						MessageHeader.STATUS,
						new ResponseMenssageListener<String>() {

							@Override
							public void onReceiver(String o) {
								response[0] = o;
								latch.countDown();
							}

							@Override
							public void onErro(String e) {
								Kernel.log(this.getClass(),e,Level.SEVERE);
								latch.countDown();
							}

							@Override
							public Class<String> responseDataClass() {
								return String.class;
							}
						}, DistributedTransactionManagerController.TRANSACTION_MANAGER_TIMEOUT);

				latch.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (SocketTimeoutException e) {
				e.printStackTrace();
			}
		}
		
		return response[0];
	}
	
	public ITable execute(String sql){
		if(id==null || id.isEmpty()){
			Kernel.log(this.getClass(),"No Transaction open",Level.WARNING);
			return null;
		}
		if(isCommit==true)return null;
		
		final ISchema schemaManipulate = getSchema(localTransaction);
		
		final ITable[] tableResult = new ITable[1];
		
		final CountDownLatch latch = new CountDownLatch(1);

		try {
			prepareResult(sql, schemaManipulate, tableResult, latch);
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		return tableResult[0];
	}

	private void prepareResult(String sql, final ISchema schemaManipulate, final ITable[] tableResult,
			final CountDownLatch latch) {
		QueryMenssage q = new QueryMenssage(id,sql);
		try {
			connection.getTransactionManager().getDispatcher().sendResquest(
					connection.getAddress(),
					connection.getPort(),
					q, 
					MessageHeader.EXECUTE_QUERY_SQL,
					new ResponseMenssageListener<ResultMenssage>() {

						@Override
						public void onReceiver(ResultMenssage r) {
							
						//	LogError.save(this.getClass(),r.getTupleData() + "<<<<<<<<<<<<<<<");

							switch (r.getState()) {
							case ResultMenssage.START_END_STATE:
								tableResult[0] = getTempTable(schemaManipulate, new Column("result","varchar"));
								connection.getTransactionManager().getDispatcher().closeListener(this);
								latch.countDown();
								break;
							case ResultMenssage.START_STATE:
								tableResult[0] = getTempTable(schemaManipulate, TableManipulate.stringToColumns(r.getHeader()));
								break;
							case ResultMenssage.END_STATE:
								connection.getTransactionManager().getDispatcher().closeListener(this);
								latch.countDown();
								break;
							case ResultMenssage.TUPLE_STATE:
								tableResult[0].writeTuple(localTransaction, r.getTupleData());
								break;
							
							default:
								latch.countDown();
								break;
							}
						}

						@Override
						public void onErro(String e) {
							Kernel.log(this.getClass(),e,Level.SEVERE);
							latch.countDown();
						}

						@Override
						public Class<ResultMenssage> responseDataClass() {
							return ResultMenssage.class;
						}
					}, DistributedTransactionManagerController.TRANSACTION_MANAGER_TIMEOUT);
		} catch (SocketTimeoutException e) {
			e.printStackTrace();
		}
	}
	
	private ISchema getSchema(ITransaction transaction){
		return Kernel.getCatalog().getSchemabyName(transaction.getConnection().getSchemaName());
	}

	private ITable getTempTable(ISchema schemaManipulate, Column... columns ){
		return TableManipulate.getTempInstance("temp"+lastRemoteResult++, schemaManipulate, columns);
	}
	public ITransaction getLocalTransaction() {
		return localTransaction;
	}

	public void setLocalTransaction(ITransaction localTransaction) {
		this.localTransaction = localTransaction;
	}

	public String getId() {
		return id;
	}
	
	public String getTransactionGlobalID() {
		return transactionGlobalID;
	}

	public void setTransactionGlobalID(String transactionGlobalID) {
		this.transactionGlobalID = transactionGlobalID;
	}

	public ResourceManagerConnection getResourceManagerConnection() {
		return connection;
	}
}