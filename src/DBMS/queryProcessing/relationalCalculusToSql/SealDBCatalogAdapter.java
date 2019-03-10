package DBMS.queryProcessing.relationalCalculusToSql;

import java.util.HashMap;
import java.util.HashSet;

import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;

public class SealDBCatalogAdapter {
	
	
	public static HashMap<String, HashSet<String>> getDbSchema(ISchema schema) {
		
		HashMap<String, HashSet<String>> tablesAndColunms = new HashMap<String, HashSet<String>>();
		
		for (ITable table : schema.getTables()) {
			
			HashSet <String> colunms = new HashSet<String>();
			
			for (String colunm : table.getColumnNames()) {
				colunms.add(colunm);
			}
			
			tablesAndColunms.put(table.getName(), colunms);
			
		}
		
		return tablesAndColunms;
	}
	
}
