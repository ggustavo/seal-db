package DBMS.fileManager.dataAcessManager.file.data;

import java.util.ArrayList;

import DBMS.fileManager.dataAcessManager.file.DataConvert;
import DBMS.fileManager.dataAcessManager.file.ExceededSizeBlockException;

public class FileBlock{

	
	private byte[] block;
	public final static int ID_OFFSET = 0;
	public final static int SIZE_USED_OFFSET = 4;
	public final static int STATUS_OFFSET = 8;
	public final static int LSN_OFFSET = 12;
	public final static int TEMP_OFFSET = 16;
	public final static int FIRST_TUPLE_OFFSET = 20;
	private int size;
	
	public FileBlock(int size) {
		this.size = size;
		this.block = new byte[size];
		for (int i = 0; i < block.length; i++) {
			block[i] = 0;
		}
		setSizeUsed(FIRST_TUPLE_OFFSET);
	}
	public FileBlock(byte[] block){
		this.block = block;
		this.size = block.length;
		if(getSizeUsed()==0){
			setSizeUsed(FIRST_TUPLE_OFFSET);
		}
	}
	
	public void setStatus(int id) {
		DataConvert.writeBytes(block, DataConvert.intToByte(id), STATUS_OFFSET);
	}

	public int getStatus() {
		return DataConvert.byteToInt(DataConvert.readBytes(block, STATUS_OFFSET, 4));
	}
	
	public void setId(int id) {
		DataConvert.writeBytes(block, DataConvert.intToByte(id), ID_OFFSET);
	}

	public int getId() {
		return DataConvert.byteToInt(DataConvert.readBytes(block, ID_OFFSET, 4));
	}
	
	public void setLSN(int id) {
		DataConvert.writeBytes(block, DataConvert.intToByte(id), LSN_OFFSET);
	}

	public int getLSN() {
		return DataConvert.byteToInt(DataConvert.readBytes(block, LSN_OFFSET, 4));
	}
	
	public void setTemp(int temp) {
		DataConvert.writeBytes(block, DataConvert.intToByte(temp), TEMP_OFFSET);
	}

	public int isTemp() {
		return DataConvert.byteToInt(DataConvert.readBytes(block, TEMP_OFFSET, 4));
	}
	
	

	public byte[] getBlock() {
		return block;
	}

	public void setBlock(byte[] block) {
		this.block = block;
	}

	
	public int getSizeUsed() {
		return DataConvert.byteToInt(DataConvert.readBytes(block, SIZE_USED_OFFSET, 4));
	}
	
	public void setSizeUsed(int size){
		DataConvert.writeBytes(block, DataConvert.intToByte(size), SIZE_USED_OFFSET);
	}
	

	public ArrayList<FileTuple> readTuplesArray(){
		ArrayList<FileTuple> tuples = new ArrayList<>();
		int i = FIRST_TUPLE_OFFSET;
		FileTuple t;
		while((t = readTuple(block, i))!=null){	
			 i = i + t.getSizeUsed();
			 if(t.getStatus()>=0)tuples.add(t);	 
		}
		return tuples;
	}
	
	public void writeTuplesArray(ArrayList<FileTuple> tuples) throws ExceededSizeBlockException{
	
		int lastTuplesOffset = getSizeUsed();
		for (int i = 0; i < tuples.size(); i++) {
			FileTuple tuple = tuples.get(i);
			if(lastTuplesOffset + tuple.getSizeUsed() > size ){
				setSizeUsed(lastTuplesOffset);
				throw new ExceededSizeBlockException(i, lastTuplesOffset, tuple.getSizeUsed());
			}
			writeTuple(block, lastTuplesOffset, tuple);
			lastTuplesOffset+=tuple.getSizeUsed();
		}
		
		setSizeUsed(lastTuplesOffset);
	}
	public void writeTuple(FileTuple tuple) throws ExceededSizeBlockException{
			
		int lastTuplesOffset = getSizeUsed();
			if(lastTuplesOffset + tuple.getSizeUsed() > size ){
				setSizeUsed(lastTuplesOffset);
				throw new ExceededSizeBlockException(0, lastTuplesOffset, tuple.getSizeUsed());
			}
			writeTuple(block, lastTuplesOffset, tuple);
			lastTuplesOffset+=tuple.getSizeUsed();
		
		
		setSizeUsed(lastTuplesOffset);
	}

	
	
	private static FileTuple readTuple(byte[] block, int offset){
		byte[] result = DataConvert.readBytes(block, offset, 4);
		if(result==null)return null;
		int size = DataConvert.byteToInt(result);
		if(size==0)return null;
		FileTuple t = new FileTuple(DataConvert.readBytes(block, offset, size));
		return t;
	}
	
	private static void writeTuple(byte[] block, int offset, FileTuple tuple){
		DataConvert.writeBytes(block, tuple.getTuple(), offset);
	}

	
	

}
