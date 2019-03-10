package DBMS.fileManager;

public class ObjectDatabaseId {

	private String schemaID;
	private String tableID;
	private String blockID;
	private String tupleID;

	public ObjectDatabaseId(String schemaID, String tableID, String blockID, String tupleID) {
		super();
		this.schemaID = schemaID;
		this.tableID = tableID;
		this.blockID = blockID;
		this.tupleID = tupleID;
	}

	@Override
	public String toString() {
		return schemaID + "-" + tableID + "-" + blockID + "-" + tupleID;
	}

	public boolean compareSchema(ObjectDatabaseId objectDatabaseId) {
		return objectDatabaseId.schemaID.equals(schemaID);
	}

	public boolean compareTable(ObjectDatabaseId objectDatabaseId) {
		return compareSchema(objectDatabaseId) && objectDatabaseId.tableID.equals(tableID);
	}

	public boolean comparePage(ObjectDatabaseId objectDatabaseId) {
		return compareTable(objectDatabaseId) && objectDatabaseId.blockID.equals(blockID);
	}

	public boolean compareTuple(ObjectDatabaseId objectDatabaseId) {
		return comparePage(objectDatabaseId) && objectDatabaseId.tupleID.equals(tupleID);
	}

	public String getSchemaID() {
		return schemaID;
	}

	public void setSchemaID(String schemaID) {
		this.schemaID = schemaID;
	}

	public String getTableID() {
		return tableID;
	}

	public void setTableID(String tableID) {
		this.tableID = tableID;
	}

	public String getBlockID() {
		return blockID;
	}

	public void setBlockID(String blockID) {
		this.blockID = blockID;
	}

	public String getTupleID() {
		return tupleID;
	}

	public void setTupleID(String tupleID) {
		this.tupleID = tupleID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((blockID == null) ? 0 : blockID.hashCode());
		result = prime * result + ((schemaID == null) ? 0 : schemaID.hashCode());
		result = prime * result + ((tableID == null) ? 0 : tableID.hashCode());
		result = prime * result + ((tupleID == null) ? 0 : tupleID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ObjectDatabaseId other = (ObjectDatabaseId) obj;
		if (blockID == null) {
			if (other.blockID != null)
				return false;
		} else if (!blockID.equals(other.blockID))
			return false;
		if (schemaID == null) {
			if (other.schemaID != null)
				return false;
		} else if (!schemaID.equals(other.schemaID))
			return false;
		if (tableID == null) {
			if (other.tableID != null)
				return false;
		} else if (!tableID.equals(other.tableID))
			return false;
		if (tupleID == null) {
			if (other.tupleID != null)
				return false;
		} else if (!tupleID.equals(other.tupleID))
			return false;
		return true;
	}
	
	

}