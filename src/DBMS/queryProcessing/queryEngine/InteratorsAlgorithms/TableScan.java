package DBMS.queryProcessing.queryEngine.InteratorsAlgorithms;

import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.transactionManager.ITransaction;

public class TableScan{

	
	private ITable table;
	private int countTuples = -1;
	private int countBlocks = 0;
	private int atualTuple = -1;
	private int numberOfBlocks;
	private ITransaction transaction;
	
	public TableScan (ITransaction transaction,ITable table){
		this.table = table;
		this.transaction = transaction;
		//table.update(transaction);
		//LogError.save(this.getClass(),">> " +table.getNumberOfTuples());
		numberOfBlocks = table.getNumberOfBlocks(transaction);
	}
	

	public void reset(){
		countTuples = -1;
		countBlocks = 0;
		atualTuple = -1;
	}
	
	public boolean hasNextBlock(){
		return countBlocks <= numberOfBlocks;
	}
	
	public BlockScan nextBlock(){	
		
		return hasNextBlock() ? new BlockScan(transaction, table, countBlocks++) : null;
	}
	
	public ITuple nextTuple() {
		//LogError.save(this.getClass(),"CALL");
		countTuples++;
		ITuple tuple = table.getTuple(transaction,countBlocks+"-"+countTuples);
		if(tuple==null){
			countBlocks++;
			countTuples = 0;
		}else{
			atualTuple++;
			//LogError.save(this.getClass(),tuple.getData());
			return tuple;
		}
		tuple = table.getTuple(transaction,countBlocks+"-"+countTuples);
		if(tuple!=null)atualTuple++;
		//if(tuple!=null)LogError.save(this.getClass(),tuple.getData());
		if(tuple == null) {
			//System.out.println("TERMINOU");
			table.unloadCache(transaction);
		}
		return tuple;
	}

	public int getAtualBlock() {
		return countBlocks;
	}
	public int getAtualTuple() {
		
		return atualTuple;
	}	
	
	public void setTransaction(ITransaction transaction){
		this.transaction = transaction;
	}


}
