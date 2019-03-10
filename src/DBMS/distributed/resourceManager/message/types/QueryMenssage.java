package DBMS.distributed.resourceManager.message.types;

import java.io.Serializable;

public class QueryMenssage implements Serializable{


	private static final long serialVersionUID = 1L;
	private String sql;
	private String trasactionID;

	public QueryMenssage(){
		
	}
	
	public QueryMenssage(String trasactionID, String sql) {
		super();
		this.trasactionID = trasactionID;
		this.sql = sql;
	}

	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public String getTrasactionID() {
		return trasactionID;
	}
	public void setTrasactionID(String trasactionID) {
		this.trasactionID = trasactionID;
	}
	

	
}
