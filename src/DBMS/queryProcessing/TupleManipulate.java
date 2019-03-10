package DBMS.queryProcessing;
import java.io.Serializable;

public class TupleManipulate implements ITuple,Serializable{

	private static final long serialVersionUID = 1L;
	private String data[];
	private int id;
	
	public TupleManipulate(int id,String[] data) {
		this.data = data;
		this.id = id;
	}
	
	public String getColunmData(int idAtribute){
		return data[idAtribute];
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String[] getData() {
		return data;
	}

	public void setData(String data[]) {
		this.data = data;
	}

	
	
	public String getStringData() {
		String s="";
		for (String string : data) {
			s = s+string+TableManipulate.SEPARATOR;
		}
		return s;
	}
	
	
	public int getIdColumn(String columnNames[],String Column) {
		for (int i = 0; i < columnNames.length; i++) {
			if(columnNames[i].equals(Column)){
				return i;
			};
		}
		return -1;
	}

	@Override
	public String toString() {
		return getStringData();
	}


	
	

	
	
}

