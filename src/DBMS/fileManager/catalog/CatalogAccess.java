package DBMS.fileManager.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import DBMS.fileManager.ISchema;

public class CatalogAccess implements ICatalog {

	private HashMap<String,ISchema> schemaMap;
	private ISchema defaultSchema;

	public HashMap<String, ISchema> getSchemaMap() {
		return schemaMap;
	}

	public CatalogAccess(){
		schemaMap = new HashMap<String,ISchema>();
	}
		
	public void addShema(ISchema s){
		schemaMap.put(s.getName()+"", s);
	}
	
	public String getSchemaPath(String s){	
		return schemaMap.get(s).getPath();
	}
	
	public ISchema getSchemabyName(String s){	
		return schemaMap.get(s);
	}
	public ISchema getSchemabyId(String s){	
		for (ISchema schema : getShemas()) {
			if(String.valueOf(schema.getId()).equals(s))return schema;
		}
		return null;
	}
	
	public List<ISchema> getShemas(){
		return new ArrayList<ISchema>(schemaMap.values());
	}

	public ISchema getDefaultSchema() {
		return defaultSchema;
	}

	public void setDefaultSchema(ISchema s) {
		schemaMap.put(s.getName()+"", s);
		this.defaultSchema = s;
	}
	
	
}
