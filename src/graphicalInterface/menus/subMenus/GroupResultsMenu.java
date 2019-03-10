package graphicalInterface.menus.subMenus;



import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.GroupResultsOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.draw.DrawTable;


public class GroupResultsMenu extends AbstractSubMenu{

	private static final long serialVersionUID = 1L;
	
	
	private DrawPlanOperation drawPlanOperation; 
	
	
	public void update(DrawPlanOperation drawPlanOperation){
		this.drawPlanOperation = drawPlanOperation;
	}
	
	
	
	public GroupResultsMenu(DBConnection connection) {
		super(connection);
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		this.setBackground(Color.white);
		this.setLayout(new FlowLayout());
		GridBagConstraints gbc = new GridBagConstraints();

	
		JButton execButton = new JButton("Execute");
		execButton.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				execute();
			}
		});

		JButton removeOPButton = new JButton("Remove Operation");
		removeOPButton.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				removeFromPlan(drawPlanOperation);

			}
		});

		JPanel jpanel = new JPanel();
		jpanel.setLayout(new GridBagLayout());
		jpanel.setBackground(Color.white);
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.insets = new Insets(4, 4, 4, 4);
		gbc.anchor = GridBagConstraints.WEST;
		gbc.fill = GridBagConstraints.HORIZONTAL;
		jpanel.add(new JLabel(""), gbc);
		gbc.gridx = 0;
		gbc.gridy = 1;
	//	jpanel.add(edit,gbc);
		gbc.gridx = 0;
		gbc.gridy = 2;
		jpanel.add(execButton, gbc);
		gbc.gridx = 0;
		gbc.gridy = 3;
		gbc.fill = GridBagConstraints.BOTH;
		jpanel.add(removeOPButton, gbc);
		this.add(jpanel);
	}


	public void execute() {
		
		if(drawPlanOperation!=null){
		
			Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
				
				public void run(ITransaction transaction) {
					//drawPlanOperation.getPlanOperation().getPlan().setTransaction(transaction);
					
					
					
					
					GroupResultsOperation mr = (GroupResultsOperation) drawPlanOperation.getPlanOperation().copy(new Plan(transaction), null);

					mr.execute();
					for (ITable tableResult  : mr.getResults()) {
						 
						DrawTable ij = new DrawTable(transaction, tableResult);
						ij.reloadMatriz();
					}
					
					transaction.commit();
				}

				@Override
				public void onFail(ITransaction transaction, Exception e) {
					transaction.abort();
					
				}
				
			});
			
		}
		
	}



	@Override
	public void apply() {
		// TODO Auto-generated method stub
		
	}
	
}
