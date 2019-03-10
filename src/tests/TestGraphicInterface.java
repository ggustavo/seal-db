package tests;



import DBMS.Kernel;
import graphicalInterface.SettingsFrame;

public class TestGraphicInterface {

	
	public static void main(String[] args) {
		try{
			SettingsFrame frame = new SettingsFrame();
			frame.setVisible(true);		
		}catch (Exception e) {
			Kernel.exception(TestGraphicInterface.class, e);
		}
		
	}
	
	
}
