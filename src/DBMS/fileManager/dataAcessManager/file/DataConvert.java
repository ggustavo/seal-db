package DBMS.fileManager.dataAcessManager.file;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Date;

public class DataConvert {
	
	
	public static void writeBytes(byte[]container , byte[]data,int offset){
		for (int i = 0; i < data.length; i++) {
			container[i+offset] = data[i];
		}
	}

	public static byte[] readBytes(byte[]data ,int offset, int length){
	
		//if(offset>data.length-1)LogError.save(this.getClass(),"offset size is larger than bytes size");
		byte readData[] = new byte[length];
		for (int i = offset; i < length+offset; i++) {
			if(i >= data.length)return null;
			readData[i-offset] = data[i];
		}
		return readData;
	}
	
	
	public static int byteToInt(byte[] byteArray) {
		
		final ByteBuffer bb = ByteBuffer.wrap(byteArray);
	    bb.order(ByteOrder.LITTLE_ENDIAN);
	    return bb.getInt();
	} 

	
	public static String byteToString(byte[] byteArray) {
		final ByteBuffer b = ByteBuffer.wrap(byteArray);
		return new String(b.array(), Charset.defaultCharset());		
	} 
	
	
	
	public static byte[] intToByte(int i) {
		
	    final ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
	    bb.order(ByteOrder.LITTLE_ENDIAN);
	    bb.putInt(i);
	    return bb.array();
	} 
	
	public static byte[] stringToByte(String s) {	
		 char[] charArray = s.toCharArray();
		 final CharBuffer cbuf = CharBuffer.wrap(charArray);
		 final ByteBuffer bbuf = Charset.defaultCharset().encode(cbuf);
		 return bbuf.array();
	} 
	

	public static int dateToInt (Date date){
	    return (int) (date.getTime()/1000);
	}

	public static Date intToDate(int i) {
	    return new Date(((long)i)*1000L);
	}
	
}
