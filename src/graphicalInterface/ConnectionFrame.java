package graphicalInterface;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.border.EmptyBorder;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.distributed.DistributedTransactionManagerController;
import graphicalInterface.draw.DrawSchemaNavigator;
import graphicalInterface.images.ImagensController;

import javax.swing.ImageIcon;
import java.awt.event.MouseAdapter;

public class ConnectionFrame extends JFrame {
	
	
	private static ConnectionFrame connectionFrame;
	
	public static ConnectionFrame getInstance(){
		if(connectionFrame == null){
			connectionFrame = new ConnectionFrame();
		}
		return connectionFrame;
	}


	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField usertextField;
	private JPasswordField passwordField;
	private InitialFrame initialFrame;
	
	public void InterfaceSystem() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (UnsupportedLookAndFeelException e) {

		} catch (ClassNotFoundException e) {

		} catch (InstantiationException e) {

		} catch (IllegalAccessException e) {

		}
	}
	
	public ConnectionFrame() {
		InterfaceSystem();
		
		this.addWindowListener(new WindowListener() {
			
			public void windowOpened(WindowEvent e){}
			public void windowIconified(WindowEvent e){}
			public void windowDeiconified(WindowEvent e){}
			public void windowDeactivated(WindowEvent e) {}
			public void windowClosed(WindowEvent e) {}
			public void windowActivated(WindowEvent e) {}
			public void windowClosing(WindowEvent e) {
				Kernel.stop();
				System.exit(0);		
			}
		});
		
		
		
		setIconImage(ImagensController.FRAME_ICON_CONNECTION);
		setTitle("Connection");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 480, 369);
		contentPane = new JPanel();
		contentPane.setBackground(Color.WHITE);
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		GridBagLayout gbl_contentPane = new GridBagLayout();
		gbl_contentPane.columnWidths = new int[]{0, 0};
		gbl_contentPane.rowHeights = new int[]{0, 284, 0, 0};
		gbl_contentPane.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_contentPane.rowWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		contentPane.setLayout(gbl_contentPane);
		
		JPanel ppanel = new JPanel();
		ppanel.setBackground(Color.WHITE);
		GridBagConstraints gbc_ppanel = new GridBagConstraints();
		gbc_ppanel.gridheight = 2;
		gbc_ppanel.insets = new Insets(0, 0, 5, 0);
		gbc_ppanel.fill = GridBagConstraints.BOTH;
		gbc_ppanel.gridx = 0;
		gbc_ppanel.gridy = 0;
		contentPane.add(ppanel, gbc_ppanel);
		GridBagLayout gbl_ppanel = new GridBagLayout();
		gbl_ppanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_ppanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_ppanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, Double.MIN_VALUE};
		gbl_ppanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		ppanel.setLayout(gbl_ppanel);
		
		JLabel lblUser = new JLabel("User");
		GridBagConstraints gbc_lblUser = new GridBagConstraints();
		gbc_lblUser.gridwidth = 3;
		gbc_lblUser.anchor = GridBagConstraints.WEST;
		gbc_lblUser.insets = new Insets(0, 0, 5, 5);
		gbc_lblUser.gridx = 1;
		gbc_lblUser.gridy = 1;
		ppanel.add(lblUser, gbc_lblUser);
		
		usertextField = new JTextField();
		usertextField.setText("admin");
		usertextField.setHorizontalAlignment(SwingConstants.TRAILING);
		GridBagConstraints gbc_usertextField = new GridBagConstraints();
		gbc_usertextField.gridwidth = 4;
		gbc_usertextField.insets = new Insets(0, 0, 5, 5);
		gbc_usertextField.fill = GridBagConstraints.HORIZONTAL;
		gbc_usertextField.gridx = 4;
		gbc_usertextField.gridy = 1;
		ppanel.add(usertextField, gbc_usertextField);
		usertextField.setColumns(10);
		
		JLabel lblPassword = new JLabel("Password");
		
		GridBagConstraints gbc_lblPassword = new GridBagConstraints();
		gbc_lblPassword.gridwidth = 3;
		gbc_lblPassword.anchor = GridBagConstraints.WEST;
		gbc_lblPassword.insets = new Insets(0, 0, 5, 5);
		gbc_lblPassword.gridx = 1;
		gbc_lblPassword.gridy = 2;
		ppanel.add(lblPassword, gbc_lblPassword);
		
		passwordField = new JPasswordField();
		passwordField.setHorizontalAlignment(SwingConstants.TRAILING);
		GridBagConstraints gbc_passwordField = new GridBagConstraints();
		gbc_passwordField.gridwidth = 4;
		gbc_passwordField.insets = new Insets(0, 0, 5, 5);
		gbc_passwordField.fill = GridBagConstraints.HORIZONTAL;
		gbc_passwordField.gridx = 4;
		gbc_passwordField.gridy = 2;
		ppanel.add(passwordField, gbc_passwordField);
		
		JLabel lblSchema = new JLabel("Schema");
		GridBagConstraints gbc_lblSchema = new GridBagConstraints();
		gbc_lblSchema.gridwidth = 3;
		gbc_lblSchema.anchor = GridBagConstraints.WEST;
		gbc_lblSchema.insets = new Insets(0, 0, 5, 5);
		gbc_lblSchema.gridx = 1;
		gbc_lblSchema.gridy = 3;
		ppanel.add(lblSchema, gbc_lblSchema);
		
		JComboBox<String> schemaComboBox = new JComboBox<String>((Vector<String>)Kernel.getSchemasNames());
		schemaComboBox.setMaximumRowCount(12);
		GridBagConstraints gbc_schemaComboBox = new GridBagConstraints();
		gbc_schemaComboBox.gridwidth = 4;
		gbc_schemaComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_schemaComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_schemaComboBox.gridx = 4;
		gbc_schemaComboBox.gridy = 3;
		ppanel.add(schemaComboBox, gbc_schemaComboBox);
		
		schemaComboBox.addMouseListener(new MouseListener() {
			int lastSize = 0;
			public void mouseReleased(MouseEvent e) {}	
			public void mousePressed(MouseEvent e) {}
			public void mouseExited(MouseEvent e) {}
			public void mouseEntered(MouseEvent e) {
				
			}
			
			public void mouseClicked(MouseEvent e) {
				Vector<String> schemas = (Vector<String>)Kernel.getSchemasNames();
			

				if(lastSize != 0 && lastSize != schemas.size()){
					schemaComboBox.setModel(new DefaultComboBoxModel<>(schemas));					
				}
			
				lastSize = schemas.size();
			}
		});
		
		JLabel label = new JLabel("      ");
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.insets = new Insets(0, 0, 5, 5);
		gbc_label.gridx = 0;
		gbc_label.gridy = 4;
		ppanel.add(label, gbc_label);
		
		
		JButton buttonStart = new JButton("Connect");
		GridBagConstraints gbc_buttonStart = new GridBagConstraints();
		gbc_buttonStart.gridwidth = 2;
		gbc_buttonStart.anchor = GridBagConstraints.EAST;
		gbc_buttonStart.insets = new Insets(0, 0, 5, 5);
		gbc_buttonStart.gridx = 6;
		gbc_buttonStart.gridy = 4;
		ppanel.add(buttonStart, gbc_buttonStart);
		
		JLabel lblTransactionManager = new JLabel("   Transaction Manager");
		lblTransactionManager.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if(initialFrame.getTransactionFrame().isVisible()){
					initialFrame.hideTransactionFrame();					
				}else{
					initialFrame.showTransactionFrame();
				}
			}
		});
		
		JLabel lblBufferManager = new JLabel("   Buffer Manager");
		lblBufferManager.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if(initialFrame.getBufferFrame().isVisible()){
					initialFrame.hideBufferFrame();
				}else{
					initialFrame.showBufferFrame();
				}
			}
		});
		
		lblBufferManager.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/buffer.png")));
		GridBagConstraints gbc_lblBufferManager = new GridBagConstraints();
		gbc_lblBufferManager.gridwidth = 2;
		gbc_lblBufferManager.anchor = GridBagConstraints.WEST;
		gbc_lblBufferManager.insets = new Insets(0, 0, 5, 5);
		gbc_lblBufferManager.gridx = 1;
		gbc_lblBufferManager.gridy = 5;
		ppanel.add(lblBufferManager, gbc_lblBufferManager);
		
		JLabel label_2 = new JLabel("");
		label_2.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				AdvancedFrame.open();
			}
		});
		label_2.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/seal_db_logo_white_low.png")));
		GridBagConstraints gbc_label_2 = new GridBagConstraints();
		gbc_label_2.gridheight = 5;
		gbc_label_2.gridwidth = 4;
		gbc_label_2.insets = new Insets(0, 0, 0, 5);
		gbc_label_2.gridx = 4;
		gbc_label_2.gridy = 5;
		ppanel.add(label_2, gbc_label_2);
		lblTransactionManager.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/transaction.png")));
		GridBagConstraints gbc_lblTransactionManager = new GridBagConstraints();
		gbc_lblTransactionManager.gridwidth = 2;
		gbc_lblTransactionManager.anchor = GridBagConstraints.WEST;
		gbc_lblTransactionManager.insets = new Insets(0, 0, 5, 5);
		gbc_lblTransactionManager.gridx = 1;
		gbc_lblTransactionManager.gridy = 6;
		ppanel.add(lblTransactionManager, gbc_lblTransactionManager);
		
		JLabel lblRecoveryManager = new JLabel("   Recovery Manager");
		lblRecoveryManager.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				if(initialFrame.getRecoveryFrame().isVisible()){
					initialFrame.hideRecoveryFrame();
				}else{
					initialFrame.showRecoveryFrame();
				}
			}
		});
		lblRecoveryManager.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/recoveryM.png")));
		GridBagConstraints gbc_lblRecoveryManager = new GridBagConstraints();
		gbc_lblRecoveryManager.gridwidth = 2;
		gbc_lblRecoveryManager.anchor = GridBagConstraints.WEST;
		gbc_lblRecoveryManager.insets = new Insets(0, 0, 5, 5);
		gbc_lblRecoveryManager.gridx = 1;
		gbc_lblRecoveryManager.gridy = 7;
		ppanel.add(lblRecoveryManager, gbc_lblRecoveryManager);
		
		JLabel label_1 = new JLabel("   Schema Explorer");
		label_1.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent arg0) {
				JComponent treeview =  DrawSchemaNavigator.create();
		        JFrame frame = new JFrame("Schema Explorer");
		        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		        frame.setExtendedState(JFrame.MAXIMIZED_BOTH);
		        frame.setIconImage(ImagensController.FRAME_ICON_DATABASE_EYE);
		        frame.setContentPane(treeview);
		        frame.pack();
		        frame.setVisible(true);
			}
		});
		label_1.setIcon(new ImageIcon(ConnectionFrame.class.getResource("/graphicalInterface/images/database_eye.png")));
		GridBagConstraints gbc_label_1 = new GridBagConstraints();
		gbc_label_1.anchor = GridBagConstraints.NORTHWEST;
		gbc_label_1.gridwidth = 2;
		gbc_label_1.insets = new Insets(0, 0, 5, 5);
		gbc_label_1.gridx = 1;
		gbc_label_1.gridy = 8;
		ppanel.add(label_1, gbc_label_1);
		
		initialFrame = new InitialFrame();
		buttonStart.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent arg0) {
				
				
				DBConnection connection = 
						DistributedTransactionManagerController.getInstance().
						getLocalConnection
						(schemaComboBox.getSelectedItem().toString(), usertextField.getText(), new String(passwordField.getPassword()));
				
				initialFrame.addPlanLayer(connection);
			}
		});
		
		this.setVisible(true);
	}
	
	public void connectAndShowSQL(String schema, String sqlInitial){
		DBConnection connection = 
				DistributedTransactionManagerController.getInstance().
				getLocalConnection
				(schema, usertextField.getText(), new String(passwordField.getPassword()));
		
		initialFrame.addPlanLayer(connection,sqlInitial);
	}

}
