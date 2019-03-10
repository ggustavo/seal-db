package DBMS.fileManager.dataAcessManager.file.data;
import java.util.LinkedList;

import DBMS.fileManager.dataAcessManager.file.DataConvert;
import DBMS.queryProcessing.TableManipulate;

public class FileTuple {
	
	
	private byte[] tuple;
	private final static int SIZE_USED_OFFSET = 0; 
	private final static int TUPLE_ID_OFFESET = 4;
	private final static int STATUS_OFFSET = 8; 
	private final static int FIRST_COLUMN_OFFSET = 12; 
	
	
	public FileTuple(byte tuple[]) {
		this.tuple = tuple;
	}
	public FileTuple(int size) {
		this.tuple = new byte[size];
		setSizeUsed(size);
		setStatus(0);
	}
	
	
	public byte[] getTuple() {
		return tuple;
	}
	
	
	public int getStatus() {
		return DataConvert.byteToInt(DataConvert.readBytes(tuple, STATUS_OFFSET, 4));
	}
	
	public void setStatus(int status){
		DataConvert.writeBytes(tuple, DataConvert.intToByte(status), STATUS_OFFSET);
	}
	

	public int getSizeUsed() {
		return DataConvert.byteToInt(DataConvert.readBytes(tuple, SIZE_USED_OFFSET, 4));
	}
	
	public void setSizeUsed(int size){
		DataConvert.writeBytes(tuple, DataConvert.intToByte(size), SIZE_USED_OFFSET);
	}
	
	public int getTupleID() {
		return DataConvert.byteToInt(DataConvert.readBytes(tuple, TUPLE_ID_OFFESET, 4));
	}
	public void setTupleID(int tupleID) {
		DataConvert.writeBytes(tuple, DataConvert.intToByte(tupleID), TUPLE_ID_OFFESET);
	}
	
	
	public static FileTuple build(int tupleID,String s){
		s = s.trim();
		String sArray[] = s.split("\\"+TableManipulate.SEPARATOR);
		LinkedList<byte[]> list = new LinkedList<>();
		int size = FIRST_COLUMN_OFFSET;
		for (int i = 0; i < sArray.length; i++) {
			byte[] b = DataConvert.stringToByte(sArray[i]);
			list.add(b);
			size+=b.length+4;
		}
		FileTuple t = new FileTuple(size);
		t.setTupleID(tupleID);
		int i = FIRST_COLUMN_OFFSET;
	
		for (byte[] b : list) {
			DataConvert.writeBytes(t.tuple, DataConvert.intToByte(b.length), i);
			i += 4;
			DataConvert.writeBytes(t.tuple, b, i);
			i+=b.length;
		}
		return t;
	}
	
	
	
	public String[] getData(){
		
		return toString().split("\\"+TableManipulate.SEPARATOR);
	}

	public String toString(){
		int offset = FIRST_COLUMN_OFFSET;
		String column = new String();
		
		while (true){
			if(offset+4>tuple.length)break;
			int size = DataConvert.byteToInt(DataConvert.readBytes(tuple, offset, 4));
	
			if(size==0)break;
			offset+=4;
			column += DataConvert.byteToString(DataConvert.readBytes(tuple, offset, size)).trim() + TableManipulate.SEPARATOR;;
			offset+=size;
		}	
		return column;
	}
	
	
}
