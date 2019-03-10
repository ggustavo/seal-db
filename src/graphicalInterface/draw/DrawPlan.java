package graphicalInterface.draw;

import java.awt.Color;
import java.util.logging.Level;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import DBMS.Kernel;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
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
import DBMS.transactionManager.ITransaction;
import graphicalInterface.InitialFrame;
import graphicalInterface.images.ImagensController;
import graphicalInterface.menus.mainMenus.OptionsOperationsMenu;
import graphicalInterface.util.CliqueXY;



public class DrawPlan extends JPanel {

	private static final long serialVersionUID = 1L;
	private CliqueXY cliqueXY;
	private Plan plan;
	private JPanel planInternalPanel;
	private InitialFrame initialFrame;
	private OptionsOperationsMenu menuOptions;
	private JPanel panelArea;
	private ITransaction transaction;
	
	public DrawPlan(OptionsOperationsMenu menu,InitialFrame initialFrame,  JPanel panelArea) {
		this.panelArea = panelArea;
		this.menuOptions = menu;
		this.initialFrame = initialFrame;
		this.setName("Plan");
		this.setLayout(null);
		this.setBackground(Color.WHITE);
	
		planInternalPanel = new JPanel();   
		planInternalPanel.setBorder(BorderFactory.createLineBorder(Color.lightGray));
		cliqueXY = new CliqueXY(planInternalPanel, this);
        planInternalPanel.setBackground(Color.WHITE);
        planInternalPanel.setLayout(null);
        planInternalPanel.setBounds(-1000, -400, 2000, 2000);   
       
        planInternalPanel.addMouseListener(cliqueXY);
        planInternalPanel.addMouseMotionListener(cliqueXY);
        
       
        createNewPlan();
        this.add(planInternalPanel);
		
	}
	
	public void createNewPlan(){
		Plan plan = new Plan(null);
		int x = 1400;
		int y = 600;
		DrawEmptySlot s = new DrawEmptySlot(this, null, ImagensController.SLOT, ImagensController.SLOT_SELECTED,true);
		s.setBounds(x + 37, y, 50, 66);
		planInternalPanel.add(s);	
		this.setPlan(plan);
	}
	
	
	public Plan testPlan(){
		
		Plan plano = new Plan(null);
		
		ProjectionOperation projecao = new ProjectionOperation();
		plano.addOperation(projecao);
		
		
		for (int i = 0; i < 3; i++) {
			
			JoinOperation la = new JoinOperation();
			TableOperation l = new TableOperation();
		//	l.setResultLeft(ITable.createNewTable("Tabela"+i));
			l.setPlan(plano);
			l.setFather(la);
			la.setRight(l);
			plano.addOperation(la);
					
		}
		
		return plano;
	}
	
	
	public void drawPlan(Plan plan) {
		planInternalPanel.removeAll();

		if(plan != null && plan.getRoot()==null){
			int x = 1400;
			int y = 600;
			DrawEmptySlot s = new DrawEmptySlot(this, null, ImagensController.SLOT, ImagensController.SLOT_SELECTED,true);
			s.setBounds(x + 37, y, 50, 66);
			planInternalPanel.add(s);	
		}
		
		if (plan != null) {
			this.setPlan(plan);
			
			int x = 1400;
			int y = 600;
			
			AbstractPlanOperation node = plan.getRoot();
			while (node != null) {

				if (node instanceof JoinOperation || node instanceof UnionOperation || node instanceof GroupResultsOperation || node instanceof IntersectionOperation) {

					if(node instanceof JoinOperation){
						DrawPlanOperation join = new DrawPlanOperation(this,node,ImagensController.JOIN,ImagensController.JOIN_SELECTED);
						join.setBounds(x, y, 120, 75);
						planInternalPanel.add(join);
					}else if (node instanceof UnionOperation){
						DrawPlanOperation union = new DrawPlanOperation(this,node,ImagensController.UNION,ImagensController.UNION_SELECTED);
						union.setBounds(x, y, 120, 75);
						planInternalPanel.add(union);
					}else if(node instanceof GroupResultsOperation){
						DrawPlanOperation results = new DrawPlanOperation(this,node,ImagensController.GROUP_RESULTS,ImagensController.GROUP_RESULTS_SELECTED);
						results.setBounds(x, y, 120, 75);
						planInternalPanel.add(results);
					}else if(node instanceof IntersectionOperation){
						DrawPlanOperation results = new DrawPlanOperation(this,node,ImagensController.INTERSECTION,ImagensController.INTERSECTION_SELECTED);
						results.setBounds(x, y, 120, 75);
						planInternalPanel.add(results);
					}
					
					AbstractPlanOperation rightNode = node.getRight();
					int y2 = y;
					
					if(rightNode == null){
						DrawEmptySlot s = new DrawEmptySlot(this, node, ImagensController.SLOT, ImagensController.SLOT_SELECTED,false);
						s.setBounds(x + 133, y2 + 75, 50, 66);
						planInternalPanel.add(s);
						y2 = y2 + 66;
					}
				
					while (rightNode != null) {
						
						if (rightNode instanceof JoinOperation) {

							Kernel.log(this.getClass()," Join right",Level.WARNING);

						}
						if (rightNode instanceof UnionOperation) {

							Kernel.log(this.getClass()," Union right",Level.WARNING);

						}
						if (rightNode instanceof GroupResultsOperation) {

							Kernel.log(this.getClass()," Materialize Results right",Level.WARNING);

						}
						if (rightNode instanceof IntersectionOperation) {

							Kernel.log(this.getClass()," Intersection Results right",Level.WARNING);

						}
						
						if (rightNode instanceof TableOperation) {
							DrawPlanOperation table = new DrawPlanOperation(this,rightNode,ImagensController.TABLE,ImagensController.TABLE_SELECTED);
							table.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(table);
							y2 = y2 + 66;
								JLabel name = new JLabel("");
								name.setBounds(x + 133, y2 + 30, 100, 70);
								table.setLabelName(name);
								if(rightNode.getResultLeft()!=null)name.setText(rightNode.getName());
								planInternalPanel.add(name);
							
						}
						
						
						if (rightNode instanceof SubplanOperation) {
							DrawPlanOperation subplan = new DrawPlanOperation(this,rightNode,ImagensController.SUBPLAN,ImagensController.SUBPLAN_SELECTED);
							subplan.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(subplan);
							y2 = y2 + 66;
								JLabel name = new JLabel("");
								name.setBounds(x + 133, y2 + 30, 100, 70);
								subplan.setLabelName(name);
								name.setText("Subplan");
								planInternalPanel.add(name);
							
						}
						
						
						if (rightNode instanceof FilterOperation) {

							DrawPlanOperation filter = new DrawPlanOperation(this,rightNode,ImagensController.FILTER,ImagensController.FILTER_SELECTED);
							filter.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(filter);
							y2 = y2 + 66;

						}
						

						if (rightNode instanceof ProjectionOperation) {

							DrawPlanOperation projection = new DrawPlanOperation(this,rightNode,ImagensController.PROJECTION,ImagensController.PROJECTION_SELECTED);
							projection.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(projection);
							y2 = y2 + 66;

						}

						if (rightNode instanceof SelectionOperation) {

							DrawPlanOperation selection = new DrawPlanOperation(this,rightNode,ImagensController.SELECTION,ImagensController.SELECTION_SELECTED);
							selection.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(selection);
							y2 = y2 + 66;

						}
						
						if(rightNode instanceof SortOperation){
							DrawPlanOperation sort = new DrawPlanOperation(this, rightNode, ImagensController.SORT, ImagensController.SORT_SELECTED);
							sort.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(sort);
							y2 = y2 + 66;
							
						}
						
						if(rightNode instanceof AggregationOperation){
							DrawPlanOperation aggregation = new DrawPlanOperation(this, rightNode, ImagensController.AGGREGATION, ImagensController.AGGREGATION_SELECTED);
							aggregation.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(aggregation);
							y2 = y2 + 66;
							
						}
						
						if(rightNode.getLeft() == null && !(rightNode instanceof TableOperation) && !(rightNode instanceof SubplanOperation)){
							DrawEmptySlot s = new DrawEmptySlot(this, rightNode, ImagensController.SLOT, ImagensController.SLOT_SELECTED,false);
							s.setBounds(x + 133, y2 + 75, 50, 66);
							planInternalPanel.add(s);
							y2 = y2 + 66;
			
						}
						

						rightNode = rightNode.getLeft();
					}
					
					x = x - 100;
					y = y + 75;
				}

				if (node instanceof TableOperation) {
					DrawPlanOperation table = new DrawPlanOperation(this,node,ImagensController.TABLE,ImagensController.TABLE_SELECTED);
					table.setBounds(x + 37, y, 50, 66);
					planInternalPanel.add(table);
					y = y + 75;
					
						JLabel name = new JLabel("");
						name.setBounds(x + 40, y - 54, 100, 70);
						if(node.getResultLeft()!=null)name.setText(node.getName());
						table.setLabelName(name);				
						planInternalPanel.add(name);
					
					x = x - 100;
				}
				
				if (node instanceof SubplanOperation) {
					DrawPlanOperation subplan = new DrawPlanOperation(this,node,ImagensController.SUBPLAN,ImagensController.SUBPLAN_SELECTED);
					subplan.setBounds(x + 37, y, 50, 66);
					planInternalPanel.add(subplan);
					y = y + 75;
					
						JLabel name = new JLabel("");
						name.setBounds(x + 40, y - 54, 100, 70);
						name.setText("Subplan");
						subplan.setLabelName(name);				
						planInternalPanel.add(name);
					
					x = x - 100;
				}
					
						

				if (node instanceof SelectionOperation) {
					DrawPlanOperation selection = new DrawPlanOperation(this,node,ImagensController.SELECTION,ImagensController.SELECTION_SELECTED);
					
					selection.setBounds(x + 37, y, 50, 66);
					planInternalPanel.add(selection);
					y = y + 66;

				}

				if (node instanceof ProjectionOperation) {
					DrawPlanOperation projection = new DrawPlanOperation(this,node,ImagensController.PROJECTION,ImagensController.PROJECTION_SELECTED);
					projection.setBounds(x + 37, y, 50, 66);

					planInternalPanel.add(projection);
					y = y + 66;
				}
				
				if (node instanceof FilterOperation) {
					DrawPlanOperation projection = new DrawPlanOperation(this,node,ImagensController.FILTER,ImagensController.FILTER_SELECTED);
					projection.setBounds(x + 37, y, 50, 66);

					planInternalPanel.add(projection);
					y = y + 66;
				}
				
				if (node instanceof SortOperation) {
					DrawPlanOperation sort = new DrawPlanOperation(this,node,ImagensController.SORT,ImagensController.SORT_SELECTED);
					sort.setBounds(x + 37, y, 50, 66);
					planInternalPanel.add(sort);
					y = y + 66;
				}
				
				if (node instanceof AggregationOperation) {
					DrawPlanOperation aggregation = new DrawPlanOperation(this,node,ImagensController.AGGREGATION,ImagensController.AGGREGATION_SELECTED);
					aggregation.setBounds(x + 37, y, 50, 66);
					planInternalPanel.add(aggregation);
					y = y + 66;
				}
				
				if(node.getLeft() == null && !(node instanceof TableOperation) && !(node instanceof SubplanOperation)){
					DrawEmptySlot s = new DrawEmptySlot(this, node, ImagensController.SLOT, ImagensController.SLOT_SELECTED,true);
					s.setBounds(x + 37, y, 50, 66);
					planInternalPanel.add(s);
					y = y + 66;
				}
				
				node = node.getLeft();

			}

		}
		this.revalidate();
		this.repaint();
	}

	public Plan getPlan() {
		return plan;
	}

	public void setPlan(Plan plan) {
		this.plan = plan;
	}

	public JPanel getPlanInternalPanel() {
		return planInternalPanel;
	}


	public OptionsOperationsMenu getMenuOptions() {
		return menuOptions;
	}

	public InitialFrame getInitialFrame() {
		return initialFrame;
	}

	public void setInitialFrame(InitialFrame initialFrame) {
		this.initialFrame = initialFrame;
	}

	public JPanel getPanelArea() {
		return panelArea;
	}

	public void setPanelArea(JPanel panelArea) {
		this.panelArea = panelArea;
	}

	public ITransaction getTransaction() {
		return transaction;
	}

	public void setTransaction(ITransaction transaction) {
		this.transaction = transaction;
	}
	
	
}
