package DBMS.distributed.resourceManager.message.types;

import java.io.Serializable;

public class ResultMenssage implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String header;
	private char state;
	private String tupleData;
	
	public  transient static final char START_STATE = '1';
	public  transient static final char END_STATE = '2';
	public  transient static final char START_END_STATE = '3';
	public  transient static final char TUPLE_STATE = '4';
	public String getHeader() {
		return header;
	}
	public void setHeader(String header) {
		this.header = header;
	}
	public char getState() {
		return state;
	}
	public void setState(char state) {
		this.state = state;
	}
	public String getTupleData() {
		return tupleData;
	}
	public void setTupleData(String tupleData) {
		this.tupleData = tupleData;
	}
	public ResultMenssage(String header, char state, String tupleData) {
		super();
		this.header = header;
		this.state = state;
		this.tupleData = tupleData;
	}
	
	
	
	
	

}
