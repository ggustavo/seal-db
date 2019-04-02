//package DBMS.fileManager.dataAcessManager;
//
//import DBMS.Kernel;
//import DBMS.fileManager.SchemaManipulate;
//import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
//import DBMS.queryProcessing.PointerTable;
//
//public class DataAcess {
//
//	
//	public static void writeBlock(String id,byte[] data){
//		String s[] = id.split("-");
//		SchemaManipulate schema = Kernel.getCatalog().getSchemabyId(s[0]);
//		FileBlock fileBlock = new FileBlock(data);
//		fileBlock.setId(Integer.parseInt(s[2]));
//		PointerTable table = schema.getTableById(s[1]);
//		if(table == null)throw new NullPointerException("Table id: " +s[1]+" not found");
//		table.getFileTable().write(fileBlock);
//	}
//	
//	public static byte[] readBlock(String id){
//		String s[] = id.split("-");
//		SchemaManipulate schema = Kernel.getCatalog().getSchemabyId(s[0]);
//		/*
//		LogError.save(this.getClass(),s[1] + " TENTANDO buscar <---");
//		for (PointerTable t : schema.getTables()) {
//			LogError.save(this.getClass(),"Name: "+t.getName() + "  id: " + t.getTableID());
//		}
//		LogError.save(this.getClass(),schema.getTableById(s[1]));
//		*/
//		
//		PointerTable table = schema.getTableById(s[1]);
//		if(table == null)throw new NullPointerException("Table id: " +s[1]+" not found");
//		
//		FileBlock fileBlock = table.getFileTable().read(Integer.parseInt(s[2]));
//		
//		if(fileBlock==null){
//			fileBlock = new FileBlock(Kernel.BLOCK_SIZE);
//			fileBlock.setStatus(-1);
//		}
//		if(table.isTemp()){
//			fileBlock.setTemp(1);
//		}
//		
//		return fileBlock.getBlock();
//	}
//	
//}
