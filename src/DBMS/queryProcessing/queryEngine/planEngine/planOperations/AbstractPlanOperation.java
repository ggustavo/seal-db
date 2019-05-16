package DBMS.queryProcessing.queryEngine.planEngine.planOperations;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.MTable;
import DBMS.queryProcessing.queryEngine.AcquireLockException;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.createCommands.CreateShemaOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.createCommands.CreateTableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.deleteCommands.DropTableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.insertCommands.InsertOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.GroupResultsOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SubplanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.updateCommands.UpdateOperation;


public abstract class AbstractPlanOperation{

	protected AbstractPlanOperation left;
	protected AbstractPlanOperation right;
	protected AbstractPlanOperation father;
	protected Plan plan;
	protected String name;
	protected MTable resultLeft;
	protected MTable resultRight;
	protected List<Condition> attributesOperatorsValues;
	protected MTable resultTable;
	
	private static int STATIC_CODE = 0;
	protected int code = 0;
	
	public AbstractPlanOperation(){
		code = STATIC_CODE++;
		this.setAttributesOperatorsValues(new LinkedList<>());
	}
	
	public abstract Column[] getResultTupleStruct();
	protected abstract void executeOperation(MTable resultTable) throws AcquireLockException;

	
	public MTable execute() throws AcquireLockException{
		executeChildren();
	
		if(this instanceof DropTableOperation)return nonPersistentTableResultOperation();
		if(this instanceof CreateTableOperation)return nonPersistentTableResultOperation();
		if(this instanceof CreateShemaOperation)return nonPersistentTableResultOperation();
		if(this instanceof InsertOperation)return nonPersistentTableResultOperation();
		if(this instanceof UpdateOperation)return nonPersistentTableResultOperation();
		if(this instanceof SubplanOperation)return subPlanResultResult();
		if(this instanceof TableOperation)return resultLeft;
		if(this instanceof GroupResultsOperation) return groupResultsExecute();
		if(this instanceof ProjectionOperation &&  (((ProjectionOperation)this).getAttributesProjected()==null ||((ProjectionOperation)this).getAttributesProjected().length==0))return resultLeft;

		MTable resultTable =  MTable.getTempInstance(getName(),getResultTupleStruct());
		//LogError.save(this.getClass(),"New temp: " + resultTable.getName() + " id: " + resultTable.getTableID());
		
		executeOperation(resultTable);
		this.resultTable = resultTable;
		
		//System.out.println(getName() + " size: " + resultTable.getNumberOfTuples(getPlan().getTransaction()));
		//System.out.println(getName() + " " + resultTable.getTuples());
		return resultTable;
		
	}
	
	
	
	private MTable nonPersistentTableResultOperation() throws AcquireLockException{
		
		executeOperation(null);
		
		return null;
	}
	
	private MTable subPlanResultResult() throws AcquireLockException{
		SubplanOperation sb = ((SubplanOperation)this);
		
		if(sb.getSubplan()!=null){
			sb.getSubplan().setTransaction(plan.getTransaction());
			return sb.getSubplan().execute();
		}
		return null;
	}
	
	public MTable groupResultsExecute() throws AcquireLockException {
		
		executeOperation(null);
		
		return resultLeft;
	}
	
	
	private void executeChildren() throws AcquireLockException{
		//LogError.save(this.getClass(),"Operation: " + this.getClass().getName() + " " + left + " " + right);
		if(left!=null){
			setResultLeft(left.execute());
		}
		if(right!=null){
			setResultRight(right.execute());			
		}
	}
	

	public AbstractPlanOperation copy(Plan plan,AbstractPlanOperation father){
		AbstractPlanOperation ap = null;
		try {
			 ap = this.getClass().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			Kernel.exception(this.getClass(),e);
		}
		ap.setPlan(plan);

		if(left != null)ap.setLeft(left.copy(plan,this));
		if(right != null)ap.setRight(right.copy(plan,this));;
		if(resultLeft!= null)ap.setResultLeft(this.resultLeft);
		if(resultRight != null)ap.setResultRight(this.resultRight);
		
		if( this.father != null)ap.setFather(father);;
		if( name != null)ap.setName(name);;
		if(attributesOperatorsValues!= null && !attributesOperatorsValues.isEmpty()){
			for (Condition aov : attributesOperatorsValues) {
				ap.attributesOperatorsValues.add(aov.copy());
			}
		}
		
		
		
		return ap;
	
	}
	
	public void checkExceptions() throws Exception{
		if(resultLeft == null){
			throw new Exception("[ERR0] The resultLeft does not exist");
		}else
		if(resultRight == null && this instanceof JoinOperation){
			throw new Exception("[ERR0] The resultRight does not exist");
		}else
		if(attributesOperatorsValues.isEmpty()){
			throw new Exception("[ERR0] No columns to make operation");
		}
		for (Condition aov : attributesOperatorsValues) {
			if(resultLeft.getIdColumn(aov.getAtribute()) < 0){
				throw new Exception("[ERR0] " +resultLeft.getName()+ " does not contain the column"+aov.getAtribute());
			}else
			if(resultRight.getIdColumn(aov.getValue()) < 0 && this instanceof JoinOperation){
				throw new Exception("[ERR0] " +resultRight.getName()+ " does not contain the column"+aov.getValue());
			}
			
		}
	}
	
	public static boolean isNumeric(String number){
		try{
			Double.parseDouble(number);
		}catch(Exception e){
			return false;
		}		
		return true;
	}
	
	
	public String[] getPossiblesColumnNames(){
		if(left!=null && right==null){
			return left.getPossiblesColumnNames();
		}else if(left==null && right!=null){
			return right.getPossiblesColumnNames();
		}else if(left!=null && right!=null){
			return joinVector(left.getPossiblesColumnNames(), right.getPossiblesColumnNames());
		}
		return null;
	}
	
	public static String[] joinVector(String[] vector1, String[] vector2){	
		if(vector1!=null & vector2 ==null)return vector1;
		if(vector1==null & vector2 !=null)return vector2;
		if(vector1!=null && vector2!=null){
			String[] vetor3 = new String[vector1.length + vector2.length];  
			System.arraycopy(vector1, 0, vetor3, 0, vector1.length);  
			System.arraycopy(vector2, 0, vetor3, vector1.length, vector2.length); 
			return vetor3;
		}
	    return null;
	}
	
	public static Column[] joinVector(Column[] vector1, Column[] vector2){	
		if(vector1!=null & vector2 ==null)return vector1;
		if(vector1==null & vector2 !=null)return vector2;
		if(vector1!=null && vector2!=null){
			Column[] vetor3 = new Column[vector1.length + vector2.length];  
			System.arraycopy(vector1, 0, vetor3, 0, vector1.length);  
			System.arraycopy(vector2, 0, vetor3, vector1.length, vector2.length); 
			return vetor3;
		}
	    return null;
	}
	
	public static boolean makeComparison(String value1, String operator, String value2) {
		value1 = value1.trim().replace("'", "");
		value2 = value2.trim().replace("'", "");

		String aux[] = new String[2];
		if (operator.equals("==") || operator.equals("=")) {

			return value1.equals(value2);
		}

		if (operator.equals("!=")) {

			return !value1.equals(value2);
		}

		if (operator.equals(">")) {
			if (isNumeric(value1) && isNumeric(value2)) {
				return Double.parseDouble(value1) > Double.parseDouble(value2);
			} else {

				aux[0] = value1;
				aux[1] = value2;
				// LogError.save(this.getClass(),"0:" + a1 + "1:" + a2);

				Arrays.sort(aux);
				// LogError.save(this.getClass(),"SortOperation 0:" + aux[0]+ "1:" + aux[1]);
				if (aux[1].equals(value2)) {
					// LogError.save(this.getClass(),"false");
					return false;
				} else {
					// LogError.save(this.getClass(),"true");
					return true;
				}
			}
		}
		;

		if (operator.equals("<")) {
			if (isNumeric(value1) && isNumeric(value2)) {
				return Double.parseDouble(value1) < Double.parseDouble(value2);
			} else {
				aux[0] = value1;
				aux[1] = value2;
				Arrays.sort(aux);
				if (aux[1].equals(value1)) {
					return false;
				} else {
					return true;
				}
			}
		}
		;

		if (operator.equals(">=")) {
			if (value1.equals(value2))
				return true;

			if (isNumeric(value1) && isNumeric(value2)) {
				return Double.parseDouble(value1) >= Double.parseDouble(value2);
			} else {
				aux[0] = value1;
				aux[1] = value2;
				Arrays.sort(aux);
				if (aux[1].equals(value2)) {
					return false;
				} else {
					return true;
				}
			}
		}
		;

		if (operator.equals("<=")) {
			if (value1.equals(value2))
				return true;

			if (isNumeric(value1) && isNumeric(value2)) {
				return Double.parseDouble(value1) <= Double.parseDouble(value2);
			} else {
				aux[0] = value1;
				aux[1] = value2;
				Arrays.sort(aux);
				if (aux[1].equals(value1)) {
					return false;
				} else {
					return true;
				}
			}
		}
		;

		return false;
	}
	
	
	
	public AbstractPlanOperation getLeft() {
		return left;
	}

	public void setLeft(AbstractPlanOperation left) {
		this.left = left;
	}

	public AbstractPlanOperation getRight() {
		return right;
	}

	public void setRight(AbstractPlanOperation right) {
		this.right = right;
	}

	public AbstractPlanOperation getFather() {
		return father;
	}

	public void setFather(AbstractPlanOperation father) {
		this.father = father;
	}

	public String getName() {
		
		String name = getClass().getSimpleName().replace("Operation", "");
		
		if(left!=null && right!=null) {
			return name+code+"("+left.getName()+" x "+ right.getName()+")";
		}else if(left!=null){
			return name+code+"("+left.getName()+")";
		}else if(right!=null){
			return name+code+"("+right.getName()+")";
		}else {
			return name+code+"()";
		}
		
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public Plan getPlan() {
		return plan;
	}

	public void setPlan(Plan plan) {
		this.plan = plan;
	}

	public MTable getResultLeft() {
		return resultLeft;
	}

	public void setResultLeft(MTable resultLeft) {
		this.resultLeft = resultLeft;
	}

	public MTable getResultRight() {
		return resultRight;
	}

	public void setResultRight(MTable resultRight) {
		this.resultRight = resultRight;
	}

	public List<Condition> getAttributesOperatorsValues() {
		return attributesOperatorsValues;
	}

	public void setAttributesOperatorsValues(List<Condition> attributesOperatorsValues) {
		this.attributesOperatorsValues = attributesOperatorsValues;
	}

	
	public void setNewPlan(Plan plan) {
		this.plan = plan;
		if(left!=null)left.setNewPlan(plan);
		if(right!=null)right.setNewPlan(plan);
	}

	@Override
	public String toString() {
		
		String name = getName();
		if(name.length() > 25) {
			return name.substring(0, 24)+"...";
		}
		
		return getName();
	}

	public MTable getResultTable() {
		return resultTable;
	}

	public void setResultTable(MTable resultTable) {
		this.resultTable = resultTable;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}
	
	
	
	
	
}
