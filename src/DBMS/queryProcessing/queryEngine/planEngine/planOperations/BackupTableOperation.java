package DBMS.queryProcessing.queryEngine.planEngine.planOperations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import DBMS.Kernel;
import DBMS.fileManager.Column;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;


public class BackupTableOperation extends AbstractPlanOperation{

	

	@Override
	protected void executeOperation(ITable restultTable) {
	

		ITransaction transaction = super.getPlan().getTransaction();

		TableScan tableScan = new TableScan(transaction, resultLeft);
		
		String date = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(new Date());
		
		String fileName = File.separator+resultLeft.getName()+" "+date+".sql";
		
		File backupFile = Kernel.createFile(Kernel.BACKUP_FOLDER_PATH + fileName);
	
		
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(backupFile);
		
			String sql = "CREATE TABLE " + resultLeft.getName() + " (";
			
			
			for (int i = 0; i < resultLeft.getColumnNames().length; i++) {
				sql += resultLeft.getColumnNames()[i] + " varchar";
				if (i != resultLeft.getColumnNames().length - 1) {
					sql += ", ";
				}
			}
			
			sql+=");\n\n";
			
			fileWriter.append(sql);
			
		//	"INSERT INTO employee (id , name, salary,department_id ) values";
			
			
			sql = "INSERT INTO " + resultLeft.getName() + " (";
				
			for (int i = 0; i < resultLeft.getColumnNames().length; i++) {
				sql += resultLeft.getColumnNames()[i];
				if (i != resultLeft.getColumnNames().length - 1) {
					sql += ", ";
				}
			}
			
			sql+=") VALUES \n";
			
			fileWriter.append(sql);
			
			ITuple tuple = tableScan.nextTuple();
			while (tuple != null) {

				sql= "(";
				
				for (int i = 0; i < tuple.getData().length; i++) {
					sql+= tuple.getData()[i];
					if (i != tuple.getData().length - 1) {
						sql += ", ";
					}
				}
				sql+=")";
				
				tuple = tableScan.nextTuple();
				if(tuple!=null){
					sql+=", \n";
				}else{
					sql+=";\n";
				}
				fileWriter.append(sql);
			}
		
			fileWriter.flush();
			fileWriter.close();
			plan.setOptionalMessage("backup done new file: " + Kernel.BACKUP_FOLDER_NAME+fileName);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			plan.setOptionalMessage(e.getMessage());
		}
		
				
	}

	
	
	
	public Column[] getResultTupleStruct(){
		return resultLeft.getColumns();
	}
	
	public String getName(){
		
		return "Update"+this.hashCode()+"("+super.resultLeft.getName()+")";
	}
	
}
