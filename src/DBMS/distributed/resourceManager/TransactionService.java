package DBMS.distributed.resourceManager;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.connectionManager.Dispatcher;
import DBMS.connectionManager.ResponseMenssageListener;
import DBMS.distributed.commitprotocols.TwoPhaseProtocol;
import DBMS.distributed.resourceManager.message.MessageHeader;
import DBMS.distributed.resourceManager.message.types.LogTransactionMessage;
import DBMS.distributed.resourceManager.message.types.QueryMenssage;
import DBMS.distributed.resourceManager.message.types.ResultMenssage;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.TableManipulate;
import DBMS.queryProcessing.parse.Parse;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;

public class TransactionService {
	public final static int TRANSACTION_SERVICE_TIMEOUT = 3000;
	private Dispatcher dispatcher;
	private Character response;
	private static HashMap<String, ITransaction> transactionsPool;
	private Parse p = new Parse();
	
	public TransactionService(Dispatcher dispatcher){
		this.dispatcher = dispatcher;
		transactionsPool = new HashMap<>();
	}
	
	private DBConnection testExistConnection(String id){
		return dispatcher.getServer().getConnectionService().getConnection(id);
	}
	
	
	public void beginTransaction(MessageHeader messageHeader){
		
		try {
		String idC = messageHeader.getData(String.class);
		DBConnection connection = testExistConnection(idC);
		if(connection!=null){

			ITransaction transaction = Kernel.getExecuteTransactions().begin(connection);
			transactionsPool.put(String.valueOf(transaction.getIdT()), transaction);
			dispatcher.sendResponseRequest(messageHeader, String.valueOf(transaction.getIdT()),TRANSACTION_SERVICE_TIMEOUT);
			
		}else{
			
			messageHeader.fault = "[ERR0] Connection "+idC+" not found";
			dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TRANSACTION_SERVICE_TIMEOUT);
		}
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	/*
	@SuppressWarnings({ "static-access", "unchecked" })
	public void commitTransaction(MessageHeader messageHeader){
		
		try {
		Map<String, String> mapIDs = messageHeader.getData(Map.class);
		String idL = mapIDs.get("idL");
		ITransaction transaction = transactionsPool.get(idL);
		//PersistenceMessageService message = dispatcher.getServer().getPersistenceMessageService();
		if(transaction!=null && transaction.getState() == ITransaction.PREPARED){
			messageHeader.address.getLocalHost().getHostAddress(); //TODO: Remover essa linha
			//message.insertIntoMessageLog(messageHeader.address.getHostAddress(), messageHeader.address.getLocalHost().getHostAddress(), Integer.parseInt(mapIDs.get("idG")), TwoPhaseProtocol.COMMITED, "commited");
			transaction.commit();
			transactionsPool.remove(transaction);
			dispatcher.sendResponseRequest(messageHeader, "commited", TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}else{
			//message.insertIntoMessageLog(messageHeader.address.getHostAddress(), messageHeader.address.getLocalHost().getHostAddress(), Integer.parseInt(mapIDs.get("idG")), TwoPhaseProtocol.ABORTED, "aborted");
			//messageHeader.fault = "[ERR0] Transaction "+idL+" not found";
			dispatcher.sendResponseRequest(messageHeader, messageHeader.fault, TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}
		} catch (NumberFormatException e) {
			Kernel.exception(this.getClass(),e);
		} catch (UnknownHostException e) {
			Kernel.exception(this.getClass(),e);
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	*/
	
	@SuppressWarnings("unlikely-arg-type")
	public void commitTransaction(MessageHeader messageHeader){
		
		try {
		@SuppressWarnings("unchecked")
		Map<String, String> mapIDs = messageHeader.getData(Map.class);
		String idL = mapIDs.get("idL");
		ITransaction transaction = transactionsPool.get(idL);
	
		if(transaction!=null){
			transaction.commit();
			transactionsPool.remove(transaction);
			dispatcher.sendResponseRequest(messageHeader, "commited", TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}else{
			messageHeader.fault = "[ERR0] Transaction "+idL+" not found";
			dispatcher.sendResponseRequest(messageHeader, messageHeader.fault, TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}
		} catch (NumberFormatException e) {
			Kernel.exception(this.getClass(),e);
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	
	
	@SuppressWarnings({ "static-access", "unchecked" })
	public void prepareTransaction(MessageHeader messageHeader){
		ITransaction transaction = null;
		String idL = null;
		try {
			Map<String, String> mapIDs = messageHeader.getData(Map.class);
			idL = mapIDs.get("idL");
			transaction = transactionsPool.get(idL);
			MessageHeader aux = new MessageHeader();
			aux.setData(messageHeader.getData(Map.class));
			aux.address = messageHeader.address;
			PersistenceMessageService message = dispatcher.getServer().getPersistenceMessageService();
			if(transaction!=null){
				message.insertIntoMessageLog(messageHeader.address.getHostAddress(), messageHeader.address.getLocalHost().getHostAddress(), Integer.parseInt(mapIDs.get("idG")), TwoPhaseProtocol.VOTE_REQ_RESPONSE, "prepared");
				transaction.setState(ITransaction.PREPARED);
				dispatcher.sendResponseRequest(messageHeader,"prepared", TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
				transactionsPool.replace(idL, transaction);
				waitMessage(transaction, aux);
			
			}else{
			messageHeader.fault = "[ERR0] Transaction "+idL+" not found";
			dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}
		} catch (NumberFormatException e) {
			Kernel.exception(this.getClass(),e);
		} catch (UnknownHostException e) {
			Kernel.exception(this.getClass(),e);
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		} 	
	}
	
	//Metodo usado como listener para esperar a mensagem do lider
	private synchronized void waitMessage(ITransaction transaction, MessageHeader messageHeader){
		Runnable r = () -> {
			try {
				Thread.sleep(TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
				if(transaction.getState()== ITransaction.PREPARED){
					transaction.setState(ITransaction.WAIT);
					transactionsPool.replace(String.valueOf(transaction.getIdT()), transaction);
					terminationProtocol(messageHeader, transaction);
				}
			} catch (InterruptedException e) {
				Kernel.exception(this.getClass(),e);
			}
		};
		Thread t = new Thread(r);
		t.start();
	}
	
	/*
	@SuppressWarnings({ "unchecked", "static-access" })
	public void abortTransaction(MessageHeader messageHeader){
		try {
		Map<String, String> mapIDs = messageHeader.getData(Map.class);
		String idL = mapIDs.get("idL");
		ITransaction transaction = transactionsPool.get(idL);
		PersistenceMessageService message = dispatcher.getServer().getPersistenceMessageService();
		if(transaction!=null){
			transaction.abort();
			transactionsPool.remove(transaction);
			message.insertIntoMessageLog(messageHeader.address.getHostAddress(), messageHeader.address.getLocalHost().getHostAddress(), Integer.parseInt(mapIDs.get("idG")), TwoPhaseProtocol.ABORTED, "aborted");
			dispatcher.sendResponseRequest(messageHeader, "Abort done",TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}else{
			messageHeader.fault = "[ERR0] Transaction "+idL+" not found";
			dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		} catch (NumberFormatException e) {
			Kernel.exception(this.getClass(),e);
		} catch (UnknownHostException e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	*/
	
	@SuppressWarnings({ "unchecked", "unlikely-arg-type" })
	public void abortTransaction(MessageHeader messageHeader){
		try {
		Map<String, String> mapIDs = messageHeader.getData(Map.class);
		String idL = mapIDs.get("idL");
		ITransaction transaction = transactionsPool.get(idL);
		if(transaction!=null){
			transaction.abort();
			transactionsPool.remove(transaction);
			dispatcher.sendResponseRequest(messageHeader, "Abort done",TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}else{
			messageHeader.fault = "[ERR0] Transaction "+idL+" not found";
			dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TwoPhaseProtocol.COMMIT_PROTOCOL_TIMEOUT);
		}
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		} catch (NumberFormatException e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	
	
	@SuppressWarnings({ "unchecked", "static-access" })
	private void terminationProtocol(MessageHeader messageHeader, ITransaction transaction){
		Map<String, String> mapIDs = messageHeader.getData(Map.class);
		PersistenceMessageService message = dispatcher.getServer().getPersistenceMessageService();
		List<ITuple> tuples = message.selectTransactionLog("transaction_id='"+mapIDs.get("idG")+"'");
		final CountDownLatch latch = new CountDownLatch(1);
		loop:
		for(ITuple tuple : tuples){
			String participantIP = tuple.getColunmData(1);//Pegar apenar o IP do participante
			try {
				if(!(messageHeader.address.getLocalHost().getHostAddress().equals(participantIP.split(":")[0]))){
					
					messageHeader.address = new InetSocketAddress(participantIP.split(":")[0],Integer.parseInt(participantIP.split(":")[0])).getAddress();
					dispatcher.sendResquest(messageHeader.address, Integer.parseInt((participantIP.split(":")[1])), mapIDs.get("idG"), 
											MessageHeader.STATUS, new ResponseMenssageListener<Character>() {

												@Override
												public void onReceiver(Character o) {
													response = o;
													Kernel.log(this.getClass(),"Termination Protocol Receiver: "+o,Level.INFO);
													latch.countDown();
												}

												@Override
												public void onErro(String e) {
													latch.countDown();
												}

												@Override
												public Class<Character> responseDataClass() {
													return Character.class;
												}
											
					}, TRANSACTION_SERVICE_TIMEOUT);
					
				}
				
				if(response!= null && response == ITransaction.COMMITTED){
					transaction.commit(); 
					break loop;
				}
				
			} catch (UnknownHostException e) {
				Kernel.exception(this.getClass(),e);
			} catch (SocketTimeoutException e) {
				Kernel.exception(this.getClass(),e);
			} catch (NumberFormatException e) {
				Kernel.exception(this.getClass(),e);
			}
		}
	
	}
	
	public void statusTransaction(MessageHeader messageHeader){
		try {
		String idT = messageHeader.getData(String.class);
		ITransaction transaction = transactionsPool.get(idT);
		
		if(transaction!=null){
			dispatcher.sendResponseRequest(messageHeader, ""+transaction.getState(),TRANSACTION_SERVICE_TIMEOUT);
		}else{
			PersistenceMessageService message = dispatcher.getServer().getPersistenceMessageService();
			List<ITuple> tuples = message.selectMessageLog("transaction_id="+idT);
			boolean first = false;
			boolean second = false;
			for(ITuple tuple : tuples){
				String status = tuple.getColunmData(3);
				if(status == TwoPhaseProtocol.COMMITED){
					second = true;
				}
				if(status == TwoPhaseProtocol.VOTE_REQ_RESPONSE){
					first = true;
				}
			}
			if(second){
				dispatcher.sendResponseRequest(messageHeader, ITransaction.COMMITTED,TRANSACTION_SERVICE_TIMEOUT);
			}else if(first && !second){
				dispatcher.sendResponseRequest(messageHeader, ITransaction.WAIT,TRANSACTION_SERVICE_TIMEOUT);
			}else {
				messageHeader.fault = "[ERR0] Transaction "+idT+" not found";
				dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TRANSACTION_SERVICE_TIMEOUT);
			}
		}
		
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	
	public void logTransaction(MessageHeader messageHeader){
		try {
			LogTransactionMessage log = messageHeader.getData(LogTransactionMessage.class);
			PersistenceMessageService messageService = dispatcher.getServer().getPersistenceMessageService();
			for (String participantIp : log.getParticipantsIP()){
				messageService.insertIntoTransactionLog(log.getLeaderIP(), participantIp, Integer.parseInt(log.getTransactionID()));
			}
			dispatcher.sendResponseRequest(messageHeader,log, TRANSACTION_SERVICE_TIMEOUT);
		} catch (SocketTimeoutException e) {
			Kernel.exception(this.getClass(),e);
		}
	}
	
	public void executeQuery(MessageHeader messageHeader){
			
		QueryMenssage queryMenssage = messageHeader.getData(QueryMenssage.class);
		ITransaction transaction = transactionsPool.get(queryMenssage.getTrasactionID());
		
		if(transaction!=null){
			List<Plan> plansAux = null;
			try {
				plansAux = p.parse(queryMenssage.getSql(),Kernel.getCatalog().getSchemabyName(transaction.getConnection().getSchemaName()));
			} catch (SQLException e) {
				messageHeader.fault = "[ERR0] "+e.getMessage();
				try {
					dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TRANSACTION_SERVICE_TIMEOUT);
				} catch (SocketTimeoutException e1) {
					e1.printStackTrace();
				}
				Kernel.exception(this.getClass(),e);
				return;
			}
			List<Plan> plans = plansAux;
			
			transaction.execRunnable(new TransactionRunnable() {
				
				@Override
				public void run(ITransaction transaction) {
					
					ITable resultTable = null;
					String singleResult = null;
					for (int i = 0;i<plans.size();i++) {
						Plan plan = plans.get(i);
						
						plan.setTransaction(transaction);
						
						if(i == plans.size()-1){
							resultTable = plan.execute();
							if(plan.getOptionalMessage() != null){
								singleResult = plan.getOptionalMessage();
							}
						}else{
							plan.execute();
						}
							
					}
					
					if(singleResult != null){
						ResultMenssage single = new ResultMenssage(null, ResultMenssage.START_END_STATE, singleResult);
						dispatcher.sendMultiResponseRequest(messageHeader, single,TRANSACTION_SERVICE_TIMEOUT);
						return;
						
					}else{
						TableScan tableScan = new TableScan(transaction, resultTable);
						
						ResultMenssage start = new ResultMenssage(TableManipulate.columnsToString(resultTable.getColumns()), ResultMenssage.START_STATE, null);
						dispatcher.sendMultiResponseRequest(messageHeader, start,TRANSACTION_SERVICE_TIMEOUT);

						ITuple tuple = tableScan.nextTuple();
						
						while(tuple!=null){
							
							ResultMenssage tupleMessage = new ResultMenssage(null, ResultMenssage.TUPLE_STATE, tuple.getStringData());
							dispatcher.sendMultiResponseRequest(messageHeader, tupleMessage,TRANSACTION_SERVICE_TIMEOUT);
			
							tuple = tableScan.nextTuple();
						
						}
						
						ResultMenssage end = new ResultMenssage(null, ResultMenssage.END_STATE, null);
						dispatcher.sendMultiResponseRequest(messageHeader, end,TRANSACTION_SERVICE_TIMEOUT);
						
					}
				}
				
				@Override
				public void onFail(ITransaction transaction, Exception e) {
					//transaction.abort();
					//transactionsPool.remove(transaction);
					messageHeader.fault = "[ERR0] "+e.getMessage();
					try {
						dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TRANSACTION_SERVICE_TIMEOUT);
					} catch (SocketTimeoutException e1) {
						Kernel.exception(this.getClass(),e);
					}
					
				}
			});
		
		}else{
			
			messageHeader.fault = "[ERR0] Transaction "+queryMenssage.getTrasactionID()+" not found";
			try {
				dispatcher.sendResponseRequest(messageHeader, messageHeader.fault,TRANSACTION_SERVICE_TIMEOUT);
			} catch (SocketTimeoutException e) {
				Kernel.exception(this.getClass(),e);
			}
		}
		
	}
}
