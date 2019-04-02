package DBMS.fileManager;

import DBMS.queryProcessing.MTable;

public class Column {
	private int id;
	private String name;
	private String type;
	private int tableId;
	
	public Column(){
		
	}	

	public Column(String name, String type) {
		super();
		this.name = name;
		this.type = type;
	}

	public Column(int id, String name, String type, int tableId) {
		super();
		this.id = id;
		this.name = name;
		this.type = type;
		this.tableId = tableId;
	}
	
	
	public int getTableId() {
		return tableId;
	}

	public void setTableId(int tableId) {
		this.tableId = tableId;
	}

	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	@Override
	public String toString() {
		return toTuple();
	}

	public String toTuple() {
		return id+MTable.SEPARATOR+name+MTable.SEPARATOR+type+MTable.SEPARATOR+tableId;
	}
	
	public Column copy(){
		Column column = new Column(id,new String(name),new String(type),tableId);
		return column;
	}
}
