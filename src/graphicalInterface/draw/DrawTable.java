package graphicalInterface.draw;

import java.awt.BorderLayout;
import java.awt.Toolkit;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.ITuple;
import DBMS.queryProcessing.queryEngine.InteratorsAlgorithms.TableScan;
import DBMS.transactionManager.ITransaction;
import graphicalInterface.images.ImagensController;

public class DrawTable {

	private JFrame frame = new JFrame();
	private JTable jtable;
	private ITable table;
	private ITransaction transaction;

	public DrawTable(ITransaction transaction,ITable table) {
		/*
		LogError.save(this.getClass(),"Name: "+table.getName());
		LogError.save(this.getClass(),"Path: " +table.getPath());
		LogError.save(this.getClass(),"TableID: "+table.getTableID());
		LogError.save(this.getClass(),"ColumnStructure: "+table.getColumnStructure());
		LogError.save(this.getClass(),"ColumnNames: "+Arrays.toString(table.getColumnNames()));
		*/
		this.table = table;
		this.transaction = transaction;
		frame.setTitle("Table: " + table.getName());

	}

	private void initializeValues(String a[][], String b[]) {
		frame.setIconImage((ImagensController.FRAME_ICON_TABLE));

		TableScan tableScan = new TableScan(transaction, table);

		int i = 0;
		ITuple tuple = tableScan.nextTuple();

		
		
		while (tuple != null) {
			//LogError.save(this.getClass(),Arrays.toString(tuple.getData()));
			//LogError.save(this.getClass(),"Size: " + tuple.getData().length);
			for (int j = 0; j < a[0].length; j++) {
				//LogError.save(this.getClass(),"i: "+i+" j: " + j);
				a[i][j] = tuple.getData()[j];
			}
			tuple = tableScan.nextTuple();
			i++;
		}

	}

	public void reloadMatriz() {

		String rowData[][] = new String[table.getNumberOfTuples(transaction)][table.getColumnNames().length];
		initializeValues(rowData, table.getColumnNames());
		jtable = new JTable(rowData, table.getColumnNames()) {

			private static final long serialVersionUID = 1L;

			public boolean isCellEditable(int row, int col) {
				return false;
			}

			public boolean isCellSelected(int row, int col) {
				return false;
			}
		};
//		frame.setSize((Toolkit.getDefaultToolkit().getScreenSize().width)/2,(Toolkit.getDefaultToolkit().getScreenSize().height*5)/2);
		frame.setSize((Toolkit.getDefaultToolkit().getScreenSize().width*9)/10,(Toolkit.getDefaultToolkit().getScreenSize().height*7)/10);
		//frame.setExtendedState(JFrame.MAXIMIZED_BOTH);
		jtable.setFillsViewportHeight(true);
		JScrollPane pane = new JScrollPane();
		pane.setViewportView(jtable);
		frame.getContentPane().setLayout(new BorderLayout());
		frame.getContentPane().add(pane);
		frame.setVisible(true);

	}
	
	
}
