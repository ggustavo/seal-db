package DBMS.fileManager.dataAcessManager.file;


public class ExceededSizeBlockException extends Exception {
	
	private static final long serialVersionUID = 1L;
	private int index;
	
	public ExceededSizeBlockException(int index, int used,int total) {
		super("Exceeded size of the block, Current Size: "+used + " trying to record: " +total);
		this.setIndex(index);
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}


}
