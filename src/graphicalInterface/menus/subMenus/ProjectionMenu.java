package graphicalInterface.menus.subMenus;


import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.transactionManager.ITransaction;
import DBMS.transactionManager.TransactionRunnable;
import graphicalInterface.draw.DrawPlanOperation;
import graphicalInterface.draw.DrawTable;


public class ProjectionMenu extends AbstractSubMenu{

	private static final long serialVersionUID = 1L;
	private ArrayList<JRadioButton> fields;
	private DrawPlanOperation drawPlanOperation;
	private JPanel fieldsPanel;

	
	public void update(DrawPlanOperation drawPlanOperation){
		this.drawPlanOperation = drawPlanOperation;
		fields.clear();
		fieldsPanel.removeAll();
		if(drawPlanOperation!=null && drawPlanOperation.getPlanOperation().getLeft()!=null){
			
			String[] colunms = drawPlanOperation.getPlanOperation().getPossiblesColumnNames();
			for (int i = 0; colunms!= null && i < colunms.length ; i++) {
				JRadioButton rdb = new JRadioButton(colunms[i],false);
				rdb.setBackground(Color.white);
				fields.add(rdb);
				fieldsPanel.add(rdb);
			}
			
			if(((ProjectionOperation) drawPlanOperation.getPlanOperation()).getAttributesProjected() != null){
				
				String projectedColunms[] =((ProjectionOperation) drawPlanOperation.getPlanOperation()).getAttributesProjected();
				for (String colunmName : projectedColunms) {
					for (JRadioButton rdn : fields) {
						if(rdn.getText().equals(colunmName)){
							rdn.setSelected(true);
							break;
						}
					}
				}
			}else{
				for (JRadioButton rdn : fields) {
					rdn.setSelected(true);
				}
			}
			
			fieldsPanel.revalidate();
			fieldsPanel.repaint();
		}
	}
	
	
	
	
	public ProjectionMenu(DBConnection connection) {
		super(connection);
		
		
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		
		this.setBackground(Color.white);
		this.setLayout(new BorderLayout());
		
		fieldsPanel = new JPanel();
		fieldsPanel.setLayout(new GridLayout(50, 1));
		fieldsPanel.setBackground(Color.WHITE);
		fieldsPanel.setBorder(BorderFactory.createLineBorder(Color.black));
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.getViewport().add(fieldsPanel);
		
		fields = new ArrayList<JRadioButton>();
		
		JLabel columnLabel = new JLabel("  Projection Operation Options  ");//Column: 

		JButton executeButton = new JButton("Execute");
		executeButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				if(drawPlanOperation!=null){
					
					Kernel.getExecuteTransactions().execute(connection,new TransactionRunnable() {
						
						public void run(ITransaction transaction) {
							//drawPlanOperation.getPlanOperation().getPlan().setTransaction(transaction);
							
							ProjectionOperation projecOP = (ProjectionOperation) drawPlanOperation.getPlanOperation().copy(new Plan(transaction), null);
							
							ArrayList<String> columnsArray = new ArrayList<>();				
							for (JRadioButton jRadioButton : fields) {
								if(jRadioButton.isSelected()){
									columnsArray.add(jRadioButton.getText());
								}
							}
							if(!columnsArray.isEmpty()){
								projecOP.setAttributesProjected(columnsArray.toArray(new String[columnsArray.size()]));
							}
							
							ITable tableResult = projecOP.execute();
	
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
		});
		
		JButton removeOP = new JButton("Remove Operation");
		removeOP.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				
				removeFromPlan(drawPlanOperation);
				
			}
		});
		JButton applyButton = new JButton("Apply");
		applyButton.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				apply();
			}
		});
	
		this.add(columnLabel, BorderLayout.NORTH);
		this.add(scrollPane, BorderLayout.CENTER);
				
		GridBagConstraints gbc = new GridBagConstraints();
		JPanel jpanel = new JPanel();
		jpanel.setLayout(new GridBagLayout());
		jpanel.setBackground(Color.white);
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.insets = new Insets(4, 4 , 4, 4);
		gbc.anchor = GridBagConstraints.WEST;
		gbc.fill = GridBagConstraints.HORIZONTAL;

		jpanel.add(applyButton, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 1;
		jpanel.add(executeButton, gbc);
		
		gbc.gridx = 0;
		gbc.gridy = 2;
		gbc.fill = GridBagConstraints.BOTH;
		jpanel.add(removeOP,gbc);
		
		this.add(jpanel, BorderLayout.SOUTH);
		
	}





	public void apply() {
		if(drawPlanOperation!=null){			
			ProjectionOperation projecOP = (ProjectionOperation) drawPlanOperation.getPlanOperation();
			
			ArrayList<String> columnsArray = new ArrayList<>();				
			for (JRadioButton jRadioButton : fields) {
				if(jRadioButton.isSelected()){
					columnsArray.add(jRadioButton.getText());
				}
			}
			if(!columnsArray.isEmpty()){
				projecOP.setAttributesProjected(columnsArray.toArray(new String[columnsArray.size()]));
			}
		}
		
	}

	
	
	
	
}
