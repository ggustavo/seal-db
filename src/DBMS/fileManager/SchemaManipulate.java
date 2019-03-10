package DBMS.fileManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import DBMS.Kernel;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.TableManipulate;

public class SchemaManipulate implements ISchema {
	private HashMap<String,ITable> tables;
	private int id;
	private String name;
	private String path;
	private String TABLES_FOLDER;
	private String TEMP_FOLDER;
	private String INDEX_FOLDER;
	
	
	
	
	@Override
	public void addTable(ITable table){
		tables.put(table.getTableID()+"", table);
	}
	
	@Override
	public ITable removeTable(String name){
		
		ITable table = getTableByName(name);
		if(table!=null){
			tables.values().removeAll(Collections.singleton(table));
		}
		return table;	
	}
	
	
	public SchemaManipulate(int id, String name,String path){
		this.id = id;
		tables = new HashMap<String,ITable>();
		this.name = name;
		this.path = path;
		TABLES_FOLDER = Kernel.createDirectory(path+File.separator+"tables");
		TEMP_FOLDER = Kernel.createDirectory(path+File.separator+"temp");
		INDEX_FOLDER = Kernel.createDirectory(path+File.separator+"indexes");
		
	}
	
	@Override
	public ITable getTableById(String s){	
		
		for (ITable t : getTables()) {
			
			if(String.valueOf(t.getTableID()).equals(s) ){
				return (ITable) t;
			}
		}
		
		return null;
		//return tables.get(s.split("-")[0]);
	}
	@Override
	public ITable getTableByName(String s){	
		for (ITable t : getTables()) {
			if(t.getName().equals(s))return (ITable) t;
		}
		return null;
	}
	
	@Override
	public List<ITable> getTables(){
		return new ArrayList<ITable>(tables.values());
	}


	@Override
	public String getName() {
		return name;
	}


	@Override
	public String getPath() {
		return path;
	}


	@Override
	public String getTablesFolder() {
		return TABLES_FOLDER;
	}


	@Override
	public String getTempFolder() {
		return TEMP_FOLDER;
	}


	@Override
	public int getId() {
		return id;
	}
	
	@Override
	public String toString() {
		return id+TableManipulate.SEPARATOR+name;
	}

	public String getINDEX_FOLDER() {
		return INDEX_FOLDER;
	}



	
}
