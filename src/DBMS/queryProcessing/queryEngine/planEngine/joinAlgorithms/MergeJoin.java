package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;

import java.awt.Color;

import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class MergeJoin extends AbstractJoinAlgorithm{

public void execute(ITable resultTable) {
		
		TableScan tableScanLeft = new TableScan(transaction, tableLeft); // tabela r
		TableScan tableScanRight = new TableScan(transaction, tableRight); // tabela s

		BlockScan br = null;
		BlockScan bs = null;
		ITuple ts = null;
		ITuple tr = null;
				
		tableScanLeft.reset();
		tableScanRight.reset();
		
		br = tableScanLeft.nextBlock();
		bs = tableScanRight.nextBlock() ;

		if (br != null && bs != null){
			showBlockLeft(br);
			showBlockRight(bs);
			ts = bs.nextTuple();
			tr = br.nextTuple();
		}

	
			while ( tr != null) { //para cada tupla tR em r faça

				showTupleLeft(tr);
				while (ts != null && AbstractPlanOperation.makeComparison(tr.getColunmData(tableLeft.getIdColumn(attributesOperatorsValues.get(0).getAtribute())),">=",ts.getColunmData(tableRight.getIdColumn(attributesOperatorsValues.get(0).getValue())))) {
														
					showTupleRight(ts);
						
						if (match(tr, ts)) {
							resultTable.writeTuple(transaction, tr.getStringData() + ts.getStringData());
							showMatch(tr, ts, Color.GREEN);
						}else{
							showMatch(tr, ts,Color.RED);
						}

						ts = bs.nextTuple();
						if(ts==null){
							eraseRight();
							eraseTotalCenter();
							bs = tableScanRight.nextBlock();
							if(bs!=null){
								showBlockRight(bs);
								ts = bs.nextTuple();
							}
						}
						
					
				}
				
				tr = br.nextTuple();
				if(tr==null){
					eraseLeft();
					eraseTotalCenter();
					br = tableScanLeft.nextBlock();
					if(br!=null){
						showBlockLeft(br);
						tr = br.nextTuple();						
					}
				}
			}

		}
	
	


}
