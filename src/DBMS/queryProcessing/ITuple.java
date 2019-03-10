package DBMS.queryProcessing;



public interface ITuple{
	
	public int getId();

	public void setId(int id);

	public String[] getData();

	public void setData(String data[]);
	
	public String getColunmData(int idAtribute);
	
	public String getStringData();

	

	
}
