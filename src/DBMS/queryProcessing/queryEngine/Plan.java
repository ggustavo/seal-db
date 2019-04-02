package DBMS.queryProcessing.queryEngine;

import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.GroupResultsOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.IntersectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SubplanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.UnionOperation;
import DBMS.transactionManager.Transaction;

public class Plan {

	private AbstractPlanOperation root;
	private AbstractPlanOperation last;
	private Transaction transaction;
	private int type = 0;
	private String optionalMessage;                                 
	
	
	public static final int SELECT_TYPE = 0;
	public static final int INSERT_TYPE = 1;
	public static final int DELETE_TYPE = 2;
	public static final int UPDATE_TYPE = 3;
	public static final int CREATE_TYPE = 4;
	public static final int BACKUP_TYPE = 5;
	public static final int TRANSACTION_TYPE = 6;
	public static final int DROP_TYPE = 7;
	
	
	
	
	public Plan(Transaction transaction) {
		this.transaction = transaction;
	}

	public MTable execute() throws AcquireLockException{
		return root.execute();
	}
	
	public void addOperation(AbstractPlanOperation newOperation){
		
		newOperation.setPlan(this);
		if(root == null){
			newOperation.setFather(null);
			root = last = newOperation;
		}else{
			if(last instanceof TableOperation && (newOperation instanceof TableOperation || newOperation instanceof SubplanOperation)){
				Kernel.log(this.getClass()," Trying to make an operation before a table reading",Level.WARNING);
				return;
			}
			last.setLeft(newOperation);
			newOperation.setFather(last);
			last = newOperation;
		}
	}
	


	public void addOperation(AbstractPlanOperation sourceOperation, AbstractPlanOperation newOperation){
		
		if(newOperation instanceof TableOperation || newOperation instanceof SubplanOperation){
			Kernel.log(this.getClass()," Trying to make an operation before a table reading",Level.WARNING);
			return;
		}
		
		newOperation.setPlan(this); 
		if(sourceOperation == root){
			newOperation.setFather(null);
			newOperation.setLeft(root);
			root.setFather(newOperation);
			root = newOperation; 
		}else{
			 newOperation.setLeft(sourceOperation);

			if(sourceOperation.getFather().getLeft() == sourceOperation){ 
				
				sourceOperation.getFather().setLeft(newOperation);
				
			}else if(sourceOperation.getFather().getRight() == sourceOperation){
				if(newOperation instanceof JoinOperation || newOperation instanceof UnionOperation || newOperation instanceof GroupResultsOperation || newOperation instanceof IntersectionOperation){
					Kernel.log(this.getClass()," Operation is Right ",Level.WARNING);
					return;
				}
				sourceOperation.getFather().setRight(newOperation);
			}else{
				Kernel.log(this.getClass(),"Invalid plan",Level.WARNING);
			}
				
			newOperation.setFather(sourceOperation.getFather());
			sourceOperation.setFather(newOperation);
		}
	}
	
	public void addOperationDown(boolean direction,AbstractPlanOperation sourceOperation, AbstractPlanOperation newOperation){
		if(sourceOperation instanceof TableOperation && (newOperation instanceof TableOperation || newOperation instanceof SubplanOperation)){
			Kernel.log(this.getClass()," Trying to make an operation before a table reading",Level.WARNING);
			return;
		}
		newOperation.setPlan(this); 
		newOperation.setFather(sourceOperation);
		if(sourceOperation instanceof JoinOperation || sourceOperation instanceof UnionOperation || sourceOperation instanceof GroupResultsOperation || sourceOperation instanceof IntersectionOperation){
			if(direction){			
				sourceOperation.setLeft(newOperation);
				if(sourceOperation==last){
					last = newOperation;
				}
			}else{
				if(newOperation instanceof JoinOperation || newOperation instanceof UnionOperation || newOperation instanceof GroupResultsOperation ||  newOperation instanceof IntersectionOperation){
					Kernel.log(this.getClass()," Operation is Right ",Level.WARNING);
					return;
				}
				sourceOperation.setRight(newOperation);
			}		
		}else{
			sourceOperation.setLeft(newOperation);
			if(sourceOperation==last){
				last = newOperation;
			}
		}
	}

	
	public void removeOperation(AbstractPlanOperation op){
		
		if(op!=null){
			
			if(op.getRight()!=null){
				Kernel.log(this.getClass()," Possible chain operations right",Level.WARNING);
				return;
			}

			if(op == root){
				root = op.getLeft();
			}else{
				
				if(op.getFather().getLeft() == op){
					
					op.getFather().setLeft(op.getLeft());
				
				}else if(op.getFather().getRight() == op){
					
					op.getFather().setRight(op.getLeft());
				}
				
				if(op.getLeft()!=null)op.getLeft().setFather(op.getFather());
			}
		}
	}
	public Transaction getTransaction() {
		return transaction;
	}


	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}
	public AbstractPlanOperation getRoot() {
		return root;
	}


	public void setRoot(AbstractPlanOperation root) {
		this.root = root;
	}


	public AbstractPlanOperation getLast() {
		return last;
	}


	public void setLast(AbstractPlanOperation last) {
		this.last = last;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getOptionalMessage() {
		return optionalMessage;
	}

	public void setOptionalMessage(String optionalMessage) {
		this.optionalMessage = optionalMessage;
	}


	
}
