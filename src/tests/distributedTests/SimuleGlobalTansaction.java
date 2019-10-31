package tests.distributedTests;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

import DBMS.Kernel;
import DBMS.distributed.Branch;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.distributed.ResourceManagerConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;



public class SimuleGlobalTansaction {
	
	public static void main(String[] args) throws RemoteException, UnknownHostException, SQLException {
		startLocalNode();
		
		
		execGlobalPlan();
	}
	
	
	public static void startLocalNode(){
		Kernel.PORT = 3003;	// If you wanted to choose a specific port
		Kernel.start();
	}
	
	
	
	
public static void execGlobalPlan() throws RemoteException, SQLException, UnknownHostException {
		
		
		// Simulando a Consulta -> select * from usuarios
		
		//Logo Depois da fase de Interpreta��o do SQL, 
		//o otimizador de consulta iriar criar 3 Fragmentos de consulta, baseados
		//no catalago distribuido
		
		String transactionGlobalID = String.valueOf(Kernel.getNewID(Kernel.PROPERTIES_TRASACTION_ID));
		//Ler do arquivo a count da transacao GLOBAL
	
		String F1 = "select * from employee";
		String F2 = "select * from employee";
		String F3 = "select * from usuarios ";
	
		DistributedTransactionManagerController TM = Kernel.getTransactionManager(); 
		ArrayList<ResourceManagerConnection> resourceManagerConnections = new ArrayList<ResourceManagerConnection>();
		
		//Com o catalago o banco j� sabe para onde enviar cada fragmento
		
		//Todo Node contem um Transaction Manager e um Resource Manager
		//Iniciando Nodes que participar�o da Transa��o, cria uma cconnection com 
		//a Transa��o Global com o Resource manager de cada node
		//Voce pode da iniciar um Node executando a classe StartSealDBNode
		ResourceManagerConnection nodeA = TM.register(InetAddress.getByName("localhost"), 3000, "company", "admin", "admin");
//		ResourceManagerConnection nodeB = TM.register(InetAddress.getByName("localhost"), 3002, "company", "admin", "admin");
//		ResourceManagerConnection nodeB = TM.register(InetAddress.getByName("localhost"), 3001, "empresa", "admin", "admin");
//		ResourceManagerConnection nodeC = TM.register(InetAddress.getByName("localhost"), 3002, "empresa", "admin", "admin");

		//Iniando os Branches com os Nodes
		Branch branchNodeA = nodeA.createBranch(transactionGlobalID);
//		Branch branchNodeB = nodeB.createBranch(transactionGlobalID);
//		Branch branchNodeC = nodeC.createBranch();
		
		resourceManagerConnections.add(nodeA);
//		TM.getResourcesManagerConnection().add(nodeB);
		//TODO Apos o registro de todos os n�s, enviar msgs, para que eles se conhecam, conhecam que participam da mestra transa��o
		//TM.logMessageTransactionBranchs(resourceManagerConnections);
		
		//Executado os Braches de forma "SICRONA" um atr�s do outro
		
		branchNodeA.begin();	
		ITable tableResultBranchA = branchNodeA.execute(F1);
		
		printBranchResut(branchNodeA,tableResultBranchA);
			
//		branchNodeB.begin();
//		ITable tableResultBranchB = branchNodeB.execute(F2);
//		printBranchResut(branchNodeB,tableResultBranchB);
//		
//		branchNodeC.begin();
//		ITable tableResultBranchC = branchNodeC.execute(F3);
//		printBranchResut(branchNodeC,tableResultBranchC);
//		
		
		//Aqui poderiamos utilizar um operador de UNI�O de tabelas para formar
		//o resultado final da consulta

		
		//Caso tenha chegado ate aqui � pq todo mundo conseguiu executar sem erros
		//e aqui entra o protocolo de commit!!! 
		//porem pode acontecer algum erro, vou contruir uma forma de mostrar que houve erro ainda
		//Seja usando timeout, ou erro de SQL, e etc.....

		
//		branchNodeA.commit();
//		branchNodeB.commit();
//		branchNodeC.commit();
		
		//Execucao do protocolo de commit global
		TM.commit(resourceManagerConnections);
		//Apaga os RMC que foram usados na transa��o
		
		Kernel.stop();
		//Desligado os Resource Manager dos Node A, B, C
		//nodeA.unRegister();
		//nodeB.unRegister();
		
		//Envia o resultado final da consulta para aplica��o
	}


	public static void printBranchResut(Branch branch, ITable table) {
		System.out.println("****************************************************");
		System.out.println("Show Result for branch: " + branch.getId());
		System.out.println("****************************************************");
		if (table != null) {

			TableScan scan = new TableScan(branch.getLocalTransaction(), table);
			ITuple tuple = null;

			while ((tuple = scan.nextTuple()) != null) {
			tuple.getStringData();
			}

		}
		System.out.println("****************************************************");
		System.out.println("Ends Result for branch: " + branch.getId());
		System.out.println("****************************************************");

	}
}