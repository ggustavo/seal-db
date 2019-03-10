package DBMS.fileManager;

import java.util.List;

import DBMS.queryProcessing.ITable;

public interface ISchema {

	void addTable(ITable table);

	ITable removeTable(String name);

	ITable getTableById(String s);

	ITable getTableByName(String s);

	List<ITable> getTables();

	String getName();

	String getPath();

	String getTablesFolder();

	String getTempFolder();

	int getId();

}