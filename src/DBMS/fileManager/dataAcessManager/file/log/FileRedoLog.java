package DBMS.fileManager.dataAcessManager.file.log;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import DBMS.fileManager.dataAcessManager.file.DataConvert;

public class FileRedoLog {
	
	
	public RandomAccessFile randomAccessFile;
	
	public FileRedoLog(String file) {
		try {
			randomAccessFile = new RandomAccessFile(file, "rws");			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private final static String LOG_SEPARATOR = "ϕ";
	
	public synchronized void append(int lsn, int trasaction, char operation, String obj){

		try {
			String record =  lsn + LOG_SEPARATOR
						   + trasaction  + LOG_SEPARATOR
						   + operation  + LOG_SEPARATOR
						   + obj;
			randomAccessFile.seek(randomAccessFile.length());
			
			byte[] data = record.getBytes();
			byte[] size = DataConvert.intToByte(data.length);
			
			randomAccessFile.write(size);
			randomAccessFile.write(data);
			randomAccessFile.write(size);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	




	
	public Long getPointer(){
		try {
			return new Long(randomAccessFile.length());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	private Integer lsn = null;
	public synchronized int readLastLSN() {
		interator(new LogInterator() {
			
			@Override
			public char readRecord(int lsnRecord, int trasaction, char operation, String obj, long filePointer) {
				lsn = new Integer(lsnRecord);
				
				return LogInterator.STOP;
			}
			
			@Override
			public void error(Exception e) {
				e.printStackTrace();
				
			}
		});
		
		return lsn == null ? -1 : lsn.intValue();
	}
	
	
	public void interator(LogInterator interator) {
		interator(interator, true); 
	}
	
	public void interator(LogInterator interator, boolean end) {
		
		long currentPointer = end ? getPointer() : 0;
	
		try {
			char action = end ? LogInterator.PREV : LogInterator.NEXT;
			
			while(action != LogInterator.STOP){
								
				if (action == LogInterator.NEXT) {
					
					byte[] recordSizeBytes = new byte[4];
					recordSizeBytes = readFile(currentPointer, recordSizeBytes);
					
					if (recordSizeBytes == null) {
						return;
						//action = interator.readRecord(-1, -1, ' ', null, -1);
					}else {
						int size = DataConvert.byteToInt(DataConvert.readBytes(recordSizeBytes, 0, 4));
			
						String record = new String(readFile(currentPointer + 4, new byte[size]));
					
						String values[] = record.split(LOG_SEPARATOR);
						int lsn = Integer.parseInt(values[0]); 
						int trasaction = Integer.parseInt(values[1]); 
						char operation = values[2].charAt(0); 
						long filePointer = currentPointer;
						
						currentPointer = currentPointer + 8 + size;

						action = interator.readRecord(lsn, trasaction, operation, values[3], filePointer);
					}
					
				}else

				if (action == LogInterator.PREV) {
					
					byte[] recordSizeBytes = new byte[4];
					recordSizeBytes = readFile(currentPointer - 4, recordSizeBytes);
					
					if (recordSizeBytes == null) {
						return;
						//action = interator.readRecord(-1, -1, ' ', null, -1);
					}else {
						int size = DataConvert.byteToInt(DataConvert.readBytes(recordSizeBytes, 0, 4));
			
						String record = new String(readFile(currentPointer - 4 - size, new byte[size]));
						
						
						String values[] = record.split(LOG_SEPARATOR);
						int lsn = Integer.parseInt(values[0]); 
						int trasaction = Integer.parseInt(values[1]); 
						char operation = values[2].charAt(0); 
						long filePointer = currentPointer;
						
						currentPointer = currentPointer - 8 - size;

						action = interator.readRecord(lsn, trasaction, operation, values[3], filePointer);
							
					}
						
				}else {
					interator.error(new Exception("Invalid Log Action: " + action));
					return;
					
				}
			}
		}catch (Exception e) {
			interator.error(e);
		}
		

	}
	
	public static interface LogInterator {
		public static final char STOP = 'S';
		public static final char NEXT = 'N';
		public static final char PREV = 'P';
		
		char readRecord(int lsn, int trasaction,char operation,String obj, long filePointer);
		void error(Exception e);
	}
	
	
	
	private byte[] readFile(long pointer, byte[]block) throws IOException{
			if(pointer<0)return null;
			randomAccessFile.seek(pointer);
			int i = randomAccessFile.read(block);
			if(i==-1)return null;
			return block;
	}
	
	
	public static class LogPointer{
		long value;

		public LogPointer(long value) {
			super();
			this.value = value;
		}
		
	}
	/*
	public static void main(String[] args) throws IOException {
		FileRedoLog r = new FileRedoLog("test.log");

		//r.append(1, 22, 'I', "1|Joao|1000");
		
		r.interator(new LogInterator() {
			
			@Override
			public char readRecord(int lsn, int trasaction, char operation, String obj, long filePointer) {
				
				System.out.println("---------------------------------------");
				System.out.println("LSN: "+lsn);
				System.out.println("Transaction: T" +trasaction);
				System.out.println("operation: " +operation);
				System.out.println("log file pointer: "+filePointer);
				System.out.println("object: " +obj);
				
				if(obj == null)return LogInterator.STOP;
				
				return LogInterator.PREV;
			}
			
			@Override
			public void error(Exception e) {
				e.printStackTrace();
				
			}
		});
		
		System.out.println("finish");
	}
	*/
}
