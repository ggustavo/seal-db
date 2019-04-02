package DBMS.fileManager.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import DBMS.fileManager.Schema;
import DBMS.queryProcessing.MTable;

public class CatalogAccess {

	private HashMap<String,Schema> schemaMap;
	private Schema defaultSchema;
	private Schema tempSchema;

	public HashMap<String, Schema> getSchemaMap() {
		return schemaMap;
	}

	public CatalogAccess(){
		schemaMap = new HashMap<String,Schema>();
	}
		
	public void addShema(Schema s){
		schemaMap.put(s.getName()+"", s);
	}
	
	public void removeShema(Schema s){
		schemaMap.remove(s.getName());
	}
	
	public void removeShema(String s){
		schemaMap.remove(s);
	}
	
	public Schema getSchemabyName(String s){	
		return schemaMap.get(s);
	}
	public Schema getSchemabyId(String s){	
		for (Schema schema : getShemas()) {
			if(String.valueOf(schema.getId()).equals(s))return schema;
		}
		return null;
	}
	
	public List<Schema> getShemas(){
		return new ArrayList<Schema>(schemaMap.values());
	}

	public Schema getDefaultSchema() {
		return defaultSchema;
	}

	public void setDefaultSchema(Schema s) {
		schemaMap.put(s.getName()+"", s);
		this.defaultSchema = s;
	}


	public Schema getTempSchema() {
		return tempSchema;
	}

	public void setTempSchema(Schema s) {
		schemaMap.put(s.getName()+"", s);
		this.tempSchema = s;	
	}
	
	public String show(){
		String buffer = "";
		
		for (Schema schema : getShemas()) {
			buffer+=("Schema: " + schema.getName())+"\n";
			
			for (MTable table : schema.getTables()) {
				buffer+=("\tTable: " + table.getControlTupleString())+"\n";				
			}
		}
		
		return buffer;
	}
}
