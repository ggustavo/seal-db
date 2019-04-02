package DBMS.queryProcessing.queryEngine.planEngine.planOperations;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.transactionManager.Transaction;



public class TransactionActionsOperation extends AbstractPlanOperation {
	
	public static final char ACTION_TRANSACTION_ABORT = 'A';
	public static final char ACTION_TRANSACTION_COMMIT = 'C';
	
	private char action;
	
	protected void executeOperation(MTable resultTable) {
	
		Transaction t = plan.getTransaction();
		
		switch (action) {
		case ACTION_TRANSACTION_ABORT:
			t.abort();
			break;

		case ACTION_TRANSACTION_COMMIT:
			t.commit();
			break;
				
		default:
			Kernel.log(this.getClass(),"Invalid transaction action: " + action,Level.SEVERE);
			break;
		}
		
	}
	



	
	public char getAction() {
		return action;
	}





	public void setAction(char action) {
		this.action = action;
	}



	public Column[] getResultTupleStruct(){
		return null;
	}
	
	public String getName(){
	
		return "Transaction Operation"+this.hashCode()+"("+action+plan.getTransaction().getIdT()+")";
	}



	
		


}
