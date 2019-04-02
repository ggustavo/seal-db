//package DBMS.fileManager.dataAcessManager.file.log;
//
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//
//public class FileTempLog {
//	
//	private RandomAccessFile randomAccessFile;
//	private int size;
//	
//	public FileTempLog(String file) {
//		try {
//			randomAccessFile = new RandomAccessFile(file, "rws");			
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	public void append(FileRecord fileRecord){
//
//		try {
//		
//			randomAccessFile.seek(randomAccessFile.length());
//			randomAccessFile.write(fileRecord.getRecord());
//			size++;
//		
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	
//	
//	
//	public FileRecord readIndex(long postion){
//		try {
//			if (postion < 0)return null;
//			randomAccessFile.seek(postion * FileRecord.UPDATE_LOG_RECORD_SIZE);
//			byte[] block = new byte[FileRecord.UPDATE_LOG_RECORD_SIZE];
//			int i = randomAccessFile.read(block);
//			if (i == -1)return null;
//			FileRecord f = new FileRecord(block);
//			return f;
//		} catch (IOException e) {
//			e.printStackTrace();
//			return null;
//		}
//	}
//
//	public int getSize() {
//		return size;
//	}
//
//
//	
//}
