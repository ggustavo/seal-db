package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;



import java.awt.Color;

import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.BlockScan;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;


public class BlockNestedLoopJoin extends AbstractJoinAlgorithm{
	
	public void execute(ITable resultTable) {
		
		
		TableScan tableScanLeft = new TableScan(transaction, tableLeft); // tabela r
		TableScan tableScanRight = new TableScan(transaction, tableRight); // tabela s

		BlockScan br = null;
		BlockScan bs = null;
		ITuple ts = null;
		ITuple tr = null;
				
		tableScanLeft.reset();
		while((br = tableScanLeft.nextBlock()) != null ){ //para cada bloco Br da tabela r faça 
			
			showBlockLeft(br);	

			tableScanRight.reset();
			while((bs = tableScanRight.nextBlock())!=null){ //para cada bloco Bs da tabela s faça
				
				showBlockRight(bs);

				br.reset();
				eraseLeft();
				eraseTotalCenter();
				while((tr = br.nextTuple())!=null){ // para cada tupla tr em Br
					showTupleLeft(tr);
					bs.reset();
					eraseRight();
					eraseTotalCenter();
					while ((ts = bs.nextTuple()) != null) { // para cada tupla ts em Bs
						showTupleRight(ts);

						if (match(tr, ts)) {
							resultTable.writeTuple(transaction, tr.getStringData() + ts.getStringData());
							showMatch(tr, ts, Color.GREEN);
						} else {
							showMatch(tr, ts, Color.RED);
						}

					}

				}

			}

		}
	}

}
