package DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms;

import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.Tuple;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;

public class BlockNestedLoopJoin extends AbstractJoinAlgorithm {

	public void execute(MTable resultTable) throws AcquireLockException {

		TableScan tableScanLeft = new TableScan(transaction, tableLeft); // tabela r
		TableScan tableScanRight = new TableScan(transaction, tableRight); // tabela s

		Tuple ts = null;
		Tuple tr = null;

		tableScanLeft.reset();
		while ((ts = tableScanLeft.nextTuple()) != null) {

			tableScanRight.reset();
			while ((tr = tableScanRight.nextTuple()) != null) {

				if (match(ts, tr)) {
					resultTable.writeTuple(transaction, ts.getStringData() + tr.getStringData());
				}

			}

		}
	}

}
