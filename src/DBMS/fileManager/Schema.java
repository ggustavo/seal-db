package DBMS.fileManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import DBMS.queryProcessing.MTable;

public class Schema {
	private HashMap<String, MTable> tables;
	private int id;
	private String name;

	public synchronized void addTable(MTable table) {
		tables.put(table.getTableID() + "", table);
	}

	public synchronized MTable removeTable(String name) {

		MTable table = getTableByName(name);
		if (table != null) {
			tables.values().removeAll(Collections.singleton(table));
		}
		return table;
	}

	public synchronized MTable removeTable(MTable table) {

		if (table != null) {
			tables.values().removeAll(Collections.singleton(table));
		}
		return table;
	}

	public Schema(int id, String name) {
		this.id = id;
		tables = new HashMap<String, MTable>();
		this.name = name;
		// Kernel.log(this.getClass(),"open Schema: "+name + " id: " + id,
		// Level.CONFIG);

	}

	public synchronized MTable getTableById(String s) {

		for (MTable t : getTables()) {

			if (String.valueOf(t.getTableID()).equals(s)) {
				return t;
			}
		}

		return null;
		// return tables.get(s.split("-")[0]);
	}

	public synchronized MTable getTableByName(String s) {
		for (MTable t : getTables()) {
			if (t.getName().equals(s))
				return (MTable) t;
		}
		return null;
	}

	public synchronized List<MTable> getTables() {
		return new ArrayList<MTable>(tables.values());
	}

	public String getName() {
		return name;
	}

	public int getId() {
		return id;
	}

	@Override
	public String toString() {
		return id + MTable.SEPARATOR + name;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

}
