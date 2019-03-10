package DBMS.fileManager.catalog;

import java.util.HashMap;
import java.util.List;

import DBMS.fileManager.ISchema;

public interface ICatalog {

	HashMap<String, ISchema> getSchemaMap();

	void addShema(ISchema s);

	String getSchemaPath(String s);

	ISchema getSchemabyName(String s);

	ISchema getSchemabyId(String s);

	List<ISchema> getShemas();

	ISchema getDefaultSchema();

	void setDefaultSchema(ISchema s);
	
	static ICatalog getInstance(){
		return new CatalogAccess();
	}

}