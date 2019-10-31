package tests.distributedTests;


import DBMS.Kernel;


public class StartSealDBNode {
	
	
	public static void main(String[] args) {
		
		
	
		//Kernel.SCHEMAS_FOLDER_NAME = "schemas2";
		Kernel.PORT = 3000;	// If you wanted to choose a specific port
		
		Kernel.start();
		
		
	}
	
}
