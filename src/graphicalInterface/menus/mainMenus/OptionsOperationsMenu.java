package graphicalInterface.menus.mainMenus;


import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.util.HashMap;


import javax.swing.JLabel;
import javax.swing.JPanel;

import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.AggregationOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.FilterOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.GroupResultsOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.IntersectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SortOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SubplanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.UnionOperation;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.menus.subMenus.AbstractSubMenu;
import graphicalInterface.menus.subMenus.AggregationMenu;
import graphicalInterface.menus.subMenus.FilterMenu;
import graphicalInterface.menus.subMenus.JoinMenu;
import graphicalInterface.menus.subMenus.GroupResultsMenu;
import graphicalInterface.menus.subMenus.IntersectionMenu;
import graphicalInterface.menus.subMenus.ProjectionMenu;
import graphicalInterface.menus.subMenus.SelectionMenu;
import graphicalInterface.menus.subMenus.SortMenu;
import graphicalInterface.menus.subMenus.SubPlanMenu;
import graphicalInterface.menus.subMenus.TableMenu;
import graphicalInterface.menus.subMenus.UnionMenu;
import graphicalInterface.util.MeshLayout;


public class OptionsOperationsMenu extends JPanel{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	
	
	private HashMap<String,AbstractSubMenu> hash;
	private CardLayout c = new CardLayout();
	private JPanel cardPanel = new JPanel();
	
	public JPanel initialPanel;
	
	public OptionsOperationsMenu(DBConnection connection) {
		this.setMinimumSize(new Dimension(10, 250));
		this.setPreferredSize(new Dimension(10, 250));
		hash = new HashMap<String,AbstractSubMenu>();
		this.setLayout(new GridLayout(1, 1));
		this.add(cardPanel);
		cardPanel.setLayout(c);
		this.setBackground(Color.lightGray);
		
		
		initialPanel = new JPanel();
		initialPanel.setLayout(new MeshLayout(2, 1));
		JLabel label = new JLabel("Select an operation");
		JLabel label2 = new JLabel("to open the options menu");
		label.setHorizontalAlignment(JLabel.CENTER);
		initialPanel.add(label);
		initialPanel.add(label2);
		cardPanel.add(initialPanel,"initialMenu");
		addMenu(new TableMenu(connection),TableOperation.class.getSimpleName());
		addMenu(new SelectionMenu(connection),SelectionOperation.class.getSimpleName());
		addMenu(new ProjectionMenu(connection),ProjectionOperation.class.getSimpleName());
		addMenu(new JoinMenu(connection),JoinOperation.class.getSimpleName());
		addMenu(new SortMenu(connection),SortOperation.class.getSimpleName());
		addMenu(new AggregationMenu(connection),AggregationOperation.class.getSimpleName());
		addMenu(new UnionMenu(connection),UnionOperation.class.getSimpleName());
		addMenu(new SubPlanMenu(connection),SubplanOperation.class.getSimpleName());
		addMenu(new GroupResultsMenu(connection),GroupResultsOperation.class.getSimpleName());
		addMenu(new FilterMenu(connection),FilterOperation.class.getSimpleName());
		addMenu(new IntersectionMenu(connection),IntersectionOperation.class.getSimpleName());
	}
	
	public void addMenu(AbstractSubMenu sub,String key){
		hash.put(key, sub);
		cardPanel.add(sub,key);
		sub.setOperationsMenu(this);
	}
	
	public DrawPlanOperation last;
	public AbstractSubMenu lastMenu;
	
	public void showInitialMenu(){
		c.show(cardPanel,"initialMenu");
	}
	
	public void showMenu(DrawPlanOperation p){
		if(last!=null) {
			last.deselect();
		}
		if(lastMenu!=null) {
			try{lastMenu.apply();}catch (Exception e) {
			}
		}
		last = p;
		c.show(cardPanel, p.getPlanOperation().getClass().getSimpleName());
		
		AbstractSubMenu m = hash.get(p.getPlanOperation().getClass().getSimpleName());
		lastMenu = m;
		m.update(p);
	}
	
	
}
