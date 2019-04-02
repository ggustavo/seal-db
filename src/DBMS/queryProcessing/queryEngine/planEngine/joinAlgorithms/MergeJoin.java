package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;

import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;

public class MergeJoin extends AbstractJoinAlgorithm{

public void execute(MTable resultTable) throws AcquireLockException {
		
		TableScan tableScanLeft = new TableScan(transaction, tableLeft); // tabela r
		TableScan tableScanRight = new TableScan(transaction, tableRight); // tabela s

		Tuple ts = null;
		Tuple tr = null;
				
		tableScanLeft.reset();
		tableScanRight.reset();


		ts = tableScanRight.nextTuple();
		tr = tableScanLeft.nextTuple();
	

	
			while ( tr != null) { //para cada tupla tR em r fa�a

				
				while (ts != null && AbstractPlanOperation.makeComparison(tr.getColunmData(tableLeft.getIdColumn(attributesOperatorsValues.get(0).getAtribute())),">=",ts.getColunmData(tableRight.getIdColumn(attributesOperatorsValues.get(0).getValue())))) {
														
				
						
						if (match(tr, ts)) {
							resultTable.writeTuple(transaction, tr.getStringData() + ts.getStringData());
							
						}else{
							
						}

						ts = tableScanRight.nextTuple();
						
				}
				
				tr = tableScanLeft.nextTuple();
			}

		}
	
	


}
