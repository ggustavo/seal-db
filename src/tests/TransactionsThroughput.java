package tests;

import java.awt.BorderLayout;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import DBMS.Kernel;
import DBMS.transactionManager.Transaction;
import DBMS.transactionManager.serializationGraph.TransactionNode;


public class TransactionsThroughput {
	
	static int x = 0;

	public static void start() {
		
		// create and configure the window
		JFrame window = new JFrame();
		window.setTitle("Transaction Throughput");
		window.setSize(800, 600);
		window.setLayout(new BorderLayout());
		window.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		
		XYSeries series = new XYSeries("Transaction Execution");
		XYSeriesCollection dataset = new XYSeriesCollection(series);
		JFreeChart chart = ChartFactory.createXYLineChart("Transaction Throughput", "Time (seconds)", "Transactions", dataset);
		window.add(new ChartPanel(chart), BorderLayout.CENTER);
		
		window.setVisible(true);
		
		try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		final Thread t = new Thread(new Runnable() {
			
			@Override
			public void run() {
				boolean firts = false;
				while(true) {
					int value = 0;
					try {
						
						for (Transaction t : Transaction.transactionsList) {
							if(!t.isFinish() && t.getIdT() != 0) {
								value++;
							}
						}
						
						series.add(x++, value);
						window.repaint();
						
					}catch (Exception e) {
						
					}
					
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				//System.out.println("stop");
			}
		});
	
		t.start();
	
		// show the window
		
	}

}