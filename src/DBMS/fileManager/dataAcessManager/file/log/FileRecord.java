package DBMS.fileManager.dataAcessManager.file.log;

import java.util.Date;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.dataAcessManager.file.DataConvert;

public class FileRecord {

	
	
	
	public final static int UPDATE_LOG_RECORD_SIZE = 36 + (Kernel.BLOCK_SIZE*2) + 4;
	public final static int COMMIT_RECORD_SIZE = 16;
	public final static int ABORT_RECORD_SIZE = 16;
	public final static int CHECKPOINT_RECORD_SIZE = 16;
	public final static int BENGIN_RECORD_SIZE = 16;

	
	public final static int UPDATE_LOG_RECORD_TYPE = 100;
	public final static int COMMIT_RECORD_TYPE = 200;
	public final static int ABORT_RECORD_TYPE = 300;
	public final static int BENGIN_RECORD_TYPE = 400;
	public final static int CHECKPOINT_RECORD_TYPE = 500;
	
	
	
	private byte[] record;
		

	
	public final static int LSN_OFFSET = 0;
	public final static int TRANSACTION_ID_OFFSET = 4;
	public final static int RECORD_TYPE_OFFSET = 8;
	public final static int DATE_OFFSET = 12;
	
	public final static int SCHEMA_ID_OFFSET = 16; 
	public final static int TABLE_ID_OFFSET = 20;
	public final static int BLOCK_ID_OFFSET = 24;
	public final static int TUPLE_ID_OFFSET = 28;
	
	public final static int OPERATION_TYPE_OFFSET = 32;
	
	
	public final static int BEFORE_IMAGE_ID_OFFSET = 36;
	public final static int AFTER_IMAGE_ID_OFFSET = 36 + Kernel.BLOCK_SIZE;
	
	public final static int RECORD_TYPE_OFFSET_UPDATE_LOG_RECORD = 36 + (Kernel.BLOCK_SIZE*2);
	
	
	public FileRecord(byte[] record){
		this.record = record;
	}
	public FileRecord(int type){
		int size = 0;
		
		switch (type) {
		case UPDATE_LOG_RECORD_TYPE:
			size = UPDATE_LOG_RECORD_SIZE;
			break;
		case COMMIT_RECORD_TYPE:
			size = COMMIT_RECORD_SIZE;
			break;
		case ABORT_RECORD_TYPE:
			size = ABORT_RECORD_SIZE;
			break;
		case CHECKPOINT_RECORD_TYPE:
			size = CHECKPOINT_RECORD_SIZE;
			break;
		case BENGIN_RECORD_TYPE:
			size = BENGIN_RECORD_SIZE;
			break;
		default:
			Kernel.log(this.getClass(),"Unknown Record Type: "+type,Level.SEVERE);
			break;
		}
		this.record = new byte[size];
		setRecordType(type);
	}

	public void setLSN(int id) {
		DataConvert.writeBytes(record, DataConvert.intToByte(id), LSN_OFFSET);
	}

	public int getLSN() {
		return DataConvert.byteToInt(DataConvert.readBytes(record, LSN_OFFSET, 4));
		
	}

	public int getTransactionId() {
		return DataConvert.byteToInt(DataConvert.readBytes(record, TRANSACTION_ID_OFFSET, 4));
	}

	public void setTransactionId(int transactionId) {
		DataConvert.writeBytes(record, DataConvert.intToByte(transactionId), TRANSACTION_ID_OFFSET);
	}
	
	public Date getDate() {
		return DataConvert.intToDate(DataConvert.byteToInt(DataConvert.readBytes(record, DATE_OFFSET, 4)));
	}

	public void setDate(Date date) {
		DataConvert.writeBytes(record, DataConvert.intToByte(DataConvert.dateToInt(date)), DATE_OFFSET);
	}
	

	public int getRecordType() {
		
		if(record.length == UPDATE_LOG_RECORD_SIZE){
			return DataConvert.byteToInt(DataConvert.readBytes(record, RECORD_TYPE_OFFSET_UPDATE_LOG_RECORD, 4));		
		}
		
		return DataConvert.byteToInt(DataConvert.readBytes(record, RECORD_TYPE_OFFSET, 4));
	}

	public void setRecordType(int recordType) {
		if(record.length == UPDATE_LOG_RECORD_SIZE){
			DataConvert.writeBytes(record, DataConvert.intToByte(recordType), RECORD_TYPE_OFFSET_UPDATE_LOG_RECORD);			
		}else{
			DataConvert.writeBytes(record, DataConvert.intToByte(recordType), RECORD_TYPE_OFFSET);	
		}
	}

	public int getSchemaID() {
		return DataConvert.byteToInt(DataConvert.readBytes(record, SCHEMA_ID_OFFSET, 4));
	}

	public void setSchemaID(int schemaID) {
		DataConvert.writeBytes(record, DataConvert.intToByte(schemaID), SCHEMA_ID_OFFSET);
	}

	public int getTableID() {
		return DataConvert.byteToInt(DataConvert.readBytes(record, TABLE_ID_OFFSET, 4));
	}

	public void setTableID(int tableID) {
		DataConvert.writeBytes(record, DataConvert.intToByte(tableID), TABLE_ID_OFFSET);
	}

	public int getBlockID() {
		return DataConvert.byteToInt(DataConvert.readBytes(record, BLOCK_ID_OFFSET, 4));
	}

	public void setBlockID(int blockID) {
		DataConvert.writeBytes(record, DataConvert.intToByte(blockID), BLOCK_ID_OFFSET);
	}

	public int getTupleID() {
		return DataConvert.byteToInt(DataConvert.readBytes(record, TUPLE_ID_OFFSET, 4));
	}

	public void setTupleID(int tupleID) {
		DataConvert.writeBytes(record, DataConvert.intToByte(tupleID), TUPLE_ID_OFFSET);
	}
	
	public int getOperationType() {
		return DataConvert.byteToInt(DataConvert.readBytes(record, OPERATION_TYPE_OFFSET, 4));
	}

	public void setOperationType(int operation) {
		DataConvert.writeBytes(record, DataConvert.intToByte(operation), OPERATION_TYPE_OFFSET);
	}
	

	public byte[] getBeforeImage() {
		return DataConvert.readBytes(record, BEFORE_IMAGE_ID_OFFSET, Kernel.BLOCK_SIZE);
	}

	public void setBeforeImage(byte[] beforeImage) {
		DataConvert.writeBytes(record, beforeImage, BEFORE_IMAGE_ID_OFFSET);
	}

	public byte[] getAfterImage() {
		return DataConvert.readBytes(record, AFTER_IMAGE_ID_OFFSET, Kernel.BLOCK_SIZE);
	}

	public void setAfterImage(byte[] afterImage) {
		DataConvert.writeBytes(record, afterImage, AFTER_IMAGE_ID_OFFSET);
	}
	
	public byte[] getRecord() {
		return record;
	}

	public void setRecord(byte[] record) {
		this.record = record;
	}
	
	
}

