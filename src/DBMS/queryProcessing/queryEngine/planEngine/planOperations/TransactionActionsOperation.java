package DBMS.queryProcessing.queryEngine.planEngine.planOperations;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.transactionManager.ITransaction;



public class TransactionActionsOperation extends AbstractPlanOperation {
	
	public static final char ACTION_TRANSACTION_ABORT = 'A';
	public static final char ACTION_TRANSACTION_COMMIT = 'C';
	public static final char ACTION_TRANSACTION_ROLLBACK = 'R';
	
	private char action;
	
	protected void executeOperation(ITable resultTable) {
	
		ITransaction t = plan.getTransaction();
		
		switch (action) {
		case ACTION_TRANSACTION_ABORT:
			t.abort();
			break;

		case ACTION_TRANSACTION_COMMIT:
			t.commit();
			break;
			
		case ACTION_TRANSACTION_ROLLBACK:
			t.rollback();
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
