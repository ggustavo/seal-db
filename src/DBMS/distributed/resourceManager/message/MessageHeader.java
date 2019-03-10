package DBMS.distributed.resourceManager.message;

import java.io.Serializable;
import java.net.InetAddress;

import DBMS.connectionManager.Dispatcher;

public class MessageHeader implements Serializable{
	
	private static final long serialVersionUID = 1L;
	public  transient static final char NEW_CONNECTION = 'A';
	public  transient static final char CLOSE_CONNECTION = 'B';
	
	public  transient static final char BEGIN_TRANSACTION = 'C';
	public  transient static final char COMMIT_TRANSACTION = 'D';
	
	public  transient static final char EXECUTE_QUERY_SQL = 'E';
	public  transient static final char EXECUTE_QUERY_RESULT = 'F';
	
	public  transient static final char RESPONSE = 'G';
	public  transient static final char MULTI_RESPONSE = 'H';
	
	public  transient static final char PREPARE = 'I';
	public  transient static final char ABORT = 'J';
	
	public  transient static final char STATUS = 'O';

	public  transient static final char LOG_TRANSACTION = 'P';
	
	public char Type;
	
	private String data;
	
	public int responseCode;
	public InetAddress address;
	public int port;
	public String fault;
	
	public void setData(Object o){
		data = Dispatcher.toJson(o);
	}
	
	public <T> T getData(Class<T> c){
		return Dispatcher.fromJson(data, c);
	}
	
}
