package graphicalInterface.menus.subMenus;

import javax.swing.JPanel;

import DBMS.connectionManager.DBConnection;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.menus.mainMenus.OptionsOperationsMenu;

public abstract class AbstractSubMenu extends JPanel{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected DBConnection connection;
	
	protected OptionsOperationsMenu operationsMenu;
	
	public AbstractSubMenu(DBConnection connection){
		this.connection = connection;
	}

	public abstract void update(DrawPlanOperation planOperation);
	
	public void removeFromPlan(DrawPlanOperation planOperation){
		planOperation.getDrawPlan().getPlan().removeOperation(planOperation.getPlanOperation());
		planOperation.getDrawPlan().drawPlan(planOperation.getDrawPlan().getPlan());
		if(operationsMenu!=null)operationsMenu.showInitialMenu();
	}
	public abstract void apply();

	public OptionsOperationsMenu getOperationsMenu() {
		return operationsMenu;
	}

	public void setOperationsMenu(OptionsOperationsMenu operationsMenu) {
		this.operationsMenu = operationsMenu;
	}
	
	
}
