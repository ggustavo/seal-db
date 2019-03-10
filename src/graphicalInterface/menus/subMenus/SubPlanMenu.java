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
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SubplanOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.draw.DrawTable;


public class SubPlanMenu extends AbstractSubMenu{

	private static final long serialVersionUID = 1L;
	
	
	private DrawPlanOperation drawPlanOperation; 
	private Plan subplan;
	
	public void update(DrawPlanOperation drawPlanOperation){
		this.drawPlanOperation = drawPlanOperation;
		subplan = ((SubplanOperation)drawPlanOperation.getPlanOperation()).getSubplan();
	
	}
	
	
	
	public SubPlanMenu(DBConnection connection) {
		super(connection);
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		this.setBackground(Color.white);
		this.setLayout(new FlowLayout());
		GridBagConstraints gbc = new GridBagConstraints();

		
		JButton edit = new JButton("Edit Subplan");
		edit.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {
				if(subplan==null)subplan = new Plan(null);
				drawPlanOperation.getDrawPlan().getInitialFrame().showSubPlanEditor(connection, subplan,(SubplanOperation)drawPlanOperation.getPlanOperation());

			}
		});
		
		
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
		jpanel.add(edit,gbc);
		gbc.gridx = 0;
		gbc.gridy = 2;
		jpanel.add(execButton, gbc);
		gbc.gridx = 0;
		gbc.gridy = 3;
		gbc.fill = GridBagConstraints.BOTH;
		jpanel.add(removeOPButton, gbc);
		this.add(jpanel);
	}
	public void apply() {
		
	}

	public void execute() {
		if(drawPlanOperation!=null){
		
			Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
				
				public void run(ITransaction transaction) {
					//drawPlanOperation.getPlanOperation().getPlan().setTransaction(transaction);
					
					
					SubPlanMenu.this.subplan.setTransaction(transaction);
					
					SubplanOperation subplan = (SubplanOperation) drawPlanOperation.getPlanOperation().copy(new Plan(transaction), null);
					subplan.setSubplan(SubPlanMenu.this.subplan);
					

					ITable tableResult = subplan.execute();
					DrawTable ij = new DrawTable(transaction, tableResult);
					
					ij.reloadMatriz();
					transaction.commit();
				}

				@Override
				public void onFail(ITransaction transaction, Exception e) {
					transaction.abort();
					
				}
				
			});
			
		}
		
	}
	
}
