package graphicalInterface.draw;

import java.awt.Color;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextPane;
import javax.swing.SwingConstants;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;

import DBMS.transactionManager.ITransaction;
import graphicalInterface.images.ImagensController;

import javax.swing.border.LineBorder;
import javax.swing.border.MatteBorder;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;


public class DrawTransaction extends JPanel {

	private static final long serialVersionUID = 1L;

	private ITransaction transaction;
	private JTextPane operations;
	private JLabel close;
	private JLabel id_1;

	public DrawTransaction(ITransaction transaction, DrawTransactionGraph drawTransactionGraph) {
		this.transaction = transaction;
		setBackground(Color.WHITE);
		//this.setPreferredSize(new Dimension(152, 315));
		this.setBorder(new LineBorder(new Color(0, 0, 0)));
	
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[] {150, 30, 0};
		gridBagLayout.rowHeights = new int[]{17, 0, 0};
		gridBagLayout.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
		setLayout(gridBagLayout);
		id_1 = new JLabel("T"+transaction.getIdT());
		id_1.setFont(new Font("Tahoma", Font.PLAIN, 14));
		id_1.setHorizontalAlignment(SwingConstants.CENTER);
		GridBagConstraints gbc_id_1 = new GridBagConstraints();
		gbc_id_1.anchor = GridBagConstraints.NORTH;
		gbc_id_1.insets = new Insets(0, 0, 5, 5);
		gbc_id_1.gridx = 0;
		gbc_id_1.gridy = 0;
		add(id_1, gbc_id_1);
		
		close = new JLabel("");
		close.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
		
				drawTransactionGraph.removeTransaction(DrawTransaction.this);
			}
		});
		close.setHorizontalAlignment(SwingConstants.CENTER);
		close.setIcon(ImagensController.CLOSE);
		GridBagConstraints gbc_close = new GridBagConstraints();
		gbc_close.anchor = GridBagConstraints.WEST;
		gbc_close.insets = new Insets(0, 0, 5, 0);
		gbc_close.gridx = 1;
		gbc_close.gridy = 0;
		add(close, gbc_close);
		
		
		
		
		operations = new JTextPane();
		operations.setBackground(Color.WHITE);
		operations.setBorder(new MatteBorder(1, 0, 0, 0, (Color) new Color(0, 0, 0)));
		operations.setEditable(false);
		GridBagConstraints gbc_operations = new GridBagConstraints();
		gbc_operations.fill = GridBagConstraints.HORIZONTAL;
		gbc_operations.anchor = GridBagConstraints.NORTH;
		gbc_operations.gridwidth = 2;
		gbc_operations.gridx = 0;
		gbc_operations.gridy = 1;
		add(operations, gbc_operations);
		
		

	}


	
	
	public void appendOperation(String s) {
		   try {
		      Document doc = operations.getDocument();
		      doc.insertString(doc.getLength(), "   "+ s +"   "+"\n\r", null);
		   } catch(BadLocationException exc) {
		      exc.printStackTrace();
		   }
	}
	

	public ITransaction getTransaction() {
		return transaction;
	}

	public void setTransaction(ITransaction transaction) {
		this.transaction = transaction;
	}

	
	
}
