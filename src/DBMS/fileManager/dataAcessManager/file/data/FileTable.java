package DBMS.fileManager.dataAcessManager.file.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileTable {
	
	private int blockSize;
	

	private RandomAccessFile randomAccessFile;
	
	public FileTable(String file,int blockSize) {
		this.blockSize = blockSize;
		try {
			randomAccessFile = new RandomAccessFile(file, "rws");	
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void write(FileBlock fileBlock){
		int id = fileBlock.getId();	
		try {
			randomAccessFile.seek(id*blockSize);
			randomAccessFile.write(fileBlock.getBlock());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public FileBlock read(int id){
		try {
			
			byte[] block = new byte[blockSize]; 
			randomAccessFile.seek(id*blockSize);
			int i = randomAccessFile.read(block);
			if(i==-1)return null;
			FileBlock fileBlock = new FileBlock(block);
			fileBlock.setId(id);
			return fileBlock;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

		
	public void close() throws IOException{
		randomAccessFile.close();
	}
	

}
