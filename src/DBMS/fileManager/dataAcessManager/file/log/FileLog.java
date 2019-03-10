package DBMS.fileManager.dataAcessManager.file.log;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import DBMS.fileManager.dataAcessManager.file.DataConvert;


public class FileLog {
	
	
	private RandomAccessFile randomAccessFile;
	
	public FileLog(String file) {
		try {
			randomAccessFile = new RandomAccessFile(file, "rws");			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void append(FileRecord fileRecord){

		try {
		
			randomAccessFile.seek(randomAccessFile.length());
			randomAccessFile.write(fileRecord.getRecord());
		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

	
	public LogPointer getPointer(){
		try {
			return new LogPointer(randomAccessFile.length());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public FileRecord readPrev(LogPointer pointer){
		
		byte[] block = new byte[4];
		block = readFile(pointer.value - 4, block);
		if (block == null)return null;
		
		FileRecord record = null;

		int type = DataConvert.byteToInt(DataConvert.readBytes(block, 0, 4));
		
		if (type == FileRecord.UPDATE_LOG_RECORD_TYPE) {
			block = new byte[FileRecord.UPDATE_LOG_RECORD_SIZE];
			block = readFile(pointer.value - FileRecord.UPDATE_LOG_RECORD_SIZE, block);
			if (block == null)return null;
			record = new FileRecord(block);
			pointer.value = pointer.value - FileRecord.UPDATE_LOG_RECORD_SIZE;
			
		}else{
			block = new byte[FileRecord.COMMIT_RECORD_SIZE];
			block = readFile(pointer.value - FileRecord.COMMIT_RECORD_SIZE, block);
			if (block == null)return null;
			record = new FileRecord(block);
			pointer.value = pointer.value - FileRecord.COMMIT_RECORD_SIZE;
		}

		return record;
	
	}
	
	
	
	private byte[] readFile(long pointer, byte[]block){
		try {
			if(pointer<0)return null;
			randomAccessFile.seek(pointer);
			int i = randomAccessFile.read(block);
			if(i==-1)return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return block;
	}
	
	
	public static class LogPointer{
		long value;

		public LogPointer(long value) {
			super();
			this.value = value;
		}
		
	}
}
