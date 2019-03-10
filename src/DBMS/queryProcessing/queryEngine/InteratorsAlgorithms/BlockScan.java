package DBMS.queryProcessing.queryEngine.InteratorsAlgorithms;

import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.transactionManager.ITransaction;

public class BlockScan {

	
	private ITable table;
	private int countTuples = -1;
	private int blockId;
	private int atualTuple = -1;
	private ITransaction transaction;
	
	public BlockScan(ITransaction transaction,ITable table, int blockId) {
		this.table = table;
		this.transaction = transaction;
		this.blockId = blockId;
	}

	
	public boolean hasNextTuple() {
		return false;
	}
	
	public void reset(){
		countTuples = -1;
		atualTuple = -1;
	}
	
	public ITuple nextTuple() {
		countTuples++;
		ITuple tuple = table.getTuple(transaction,blockId+"-"+countTuples);	
		if(tuple!=null){
			atualTuple++;
			return tuple;
		}
		return null;
	}

	
	public int getAtualBlock() {
		return blockId;
	}

	
	public int getAtualTuple() {
		return atualTuple;
	}
	
	

}
