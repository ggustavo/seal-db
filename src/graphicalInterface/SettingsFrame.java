package graphicalInterface;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import DBMS.Kernel;
import DBMS.bufferManager.policies.LRU;
import graphicalInterface.images.ImagensController;

import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.Vector;
import java.awt.Color;

import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.JComboBox;
import javax.swing.JButton;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.JFormattedTextField;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;


public class SettingsFrame extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JFormattedTextField textFieldbuferSize;
	private JTextField textFieldBlockSize;
	private JTextField textFieldPort;
	
	public void InterfaceSystem() {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (UnsupportedLookAndFeelException e) {

		} catch (ClassNotFoundException e) {

		} catch (InstantiationException e) {

		} catch (IllegalAccessException e) {

		}
	}
	
	public SettingsFrame() {
		InterfaceSystem();
		JFrame.setDefaultLookAndFeelDecorated(true);
		setIconImage(ImagensController.FRAME_ICON_SETTINGS);
		setTitle("Settings");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 480, 326);
		contentPane = new JPanel();
		contentPane.setBackground(Color.WHITE);
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		GridBagLayout gbl_contentPane = new GridBagLayout();
		gbl_contentPane.columnWidths = new int[]{0, 0};
		gbl_contentPane.rowHeights = new int[]{0, 0, 0, 0};
		gbl_contentPane.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_contentPane.rowWeights = new double[]{0.0, 1.0, 0.0, Double.MIN_VALUE};
		contentPane.setLayout(gbl_contentPane);
		
		JPanel buffer = new JPanel();
		buffer.setBackground(Color.WHITE);
		GridBagConstraints gbc_buffer = new GridBagConstraints();
		gbc_buffer.gridheight = 3;
		gbc_buffer.insets = new Insets(0, 0, 5, 0);
		gbc_buffer.fill = GridBagConstraints.BOTH;
		gbc_buffer.gridx = 0;
		gbc_buffer.gridy = 0;
		contentPane.add(buffer, gbc_buffer);
		GridBagLayout gbl_buffer = new GridBagLayout();
		gbl_buffer.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_buffer.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_buffer.columnWeights = new double[]{0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_buffer.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		buffer.setLayout(gbl_buffer);
		
		JLabel label = new JLabel("          ");
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.insets = new Insets(0, 0, 5, 5);
		gbc_label.gridx = 0;
		gbc_label.gridy = 5;
		buffer.add(label, gbc_label);
		
		JLabel port = new JLabel("Port:");
		
		GridBagConstraints gbc_port = new GridBagConstraints();
		gbc_port.anchor = GridBagConstraints.WEST;
		gbc_port.insets = new Insets(0, 0, 5, 5);
		gbc_port.gridx = 2;
		gbc_port.gridy = 6;
		buffer.add(port, gbc_port);
		
		textFieldPort = new JTextField();
		textFieldPort.setText(3000+"");
		textFieldPort.setHorizontalAlignment(SwingConstants.TRAILING);
		GridBagConstraints gbc_passwordField = new GridBagConstraints();
		gbc_passwordField.gridwidth = 10;
		gbc_passwordField.insets = new Insets(0, 0, 5, 5);
		gbc_passwordField.fill = GridBagConstraints.HORIZONTAL;
		gbc_passwordField.gridx = 3;
		gbc_passwordField.gridy = 6;
		buffer.add(textFieldPort, gbc_passwordField);
		
		JLabel label_1 = new JLabel("       ");
		GridBagConstraints gbc_label_1 = new GridBagConstraints();
		gbc_label_1.insets = new Insets(0, 0, 5, 0);
		gbc_label_1.gridx = 13;
		gbc_label_1.gridy = 6;
		buffer.add(label_1, gbc_label_1);
		
		JLabel lblBufferSize = new JLabel("Buffer Size (Pages):");
		lblBufferSize.setToolTipText("Maximum Pages in Buffer");
		GridBagConstraints gbc_lblBufferSize = new GridBagConstraints();
		gbc_lblBufferSize.anchor = GridBagConstraints.WEST;
		gbc_lblBufferSize.insets = new Insets(0, 0, 5, 5);
		gbc_lblBufferSize.gridx = 2;
		gbc_lblBufferSize.gridy = 7;
		buffer.add(lblBufferSize, gbc_lblBufferSize);
		
		
		textFieldbuferSize = new JFormattedTextField( );
		textFieldbuferSize.setToolTipText("Maximum Pages in Buffer");
		textFieldbuferSize.setHorizontalAlignment(SwingConstants.TRAILING);
		textFieldbuferSize.setText(39+""); 
		GridBagConstraints gbc_TextFieldbuferSize = new GridBagConstraints();
		gbc_TextFieldbuferSize.anchor = GridBagConstraints.SOUTH;
		gbc_TextFieldbuferSize.fill = GridBagConstraints.HORIZONTAL;
		gbc_TextFieldbuferSize.gridwidth = 10;
		gbc_TextFieldbuferSize.insets = new Insets(0, 0, 5, 5);
		gbc_TextFieldbuferSize.gridx = 3;
		gbc_TextFieldbuferSize.gridy = 7;
		buffer.add(textFieldbuferSize, gbc_TextFieldbuferSize);
		textFieldbuferSize.setColumns(10);
		
		JLabel lblNewLabel = new JLabel("Buffer Policy:");
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel.gridx = 2;
		gbc_lblNewLabel.gridy = 8;
		buffer.add(lblNewLabel, gbc_lblNewLabel);
		
		
		JComboBox<String> comboBox = new JComboBox<String>((Vector<String>)Kernel.getBufferPoliciesListNames());
		comboBox.setSelectedItem(LRU.class.getSimpleName());
		comboBox.setMaximumRowCount(12);
		GridBagConstraints gbc_comboBox = new GridBagConstraints();
		gbc_comboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_comboBox.gridwidth = 10;
		gbc_comboBox.insets = new Insets(0, 0, 5, 5);
		gbc_comboBox.gridx = 3;
		gbc_comboBox.gridy = 8;
		buffer.add(comboBox, gbc_comboBox);
		
		JLabel lblNewLabel_2 = new JLabel("Block Size:");
		GridBagConstraints gbc_lblNewLabel_2 = new GridBagConstraints();
		gbc_lblNewLabel_2.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel_2.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel_2.gridx = 2;
		gbc_lblNewLabel_2.gridy = 9;
		buffer.add(lblNewLabel_2, gbc_lblNewLabel_2);
		
		textFieldBlockSize = new JTextField();
		textFieldBlockSize.setHorizontalAlignment(SwingConstants.TRAILING);
		textFieldBlockSize.setText("-"); //TODO
		textFieldBlockSize.setEnabled(false);
		GridBagConstraints gbc_TextFieldBlockSize = new GridBagConstraints();
		gbc_TextFieldBlockSize.gridwidth = 10;
		gbc_TextFieldBlockSize.insets = new Insets(0, 0, 5, 5);
		gbc_TextFieldBlockSize.fill = GridBagConstraints.HORIZONTAL;
		gbc_TextFieldBlockSize.gridx = 3;
		gbc_TextFieldBlockSize.gridy = 9;
		buffer.add(textFieldBlockSize, gbc_TextFieldBlockSize);
		textFieldBlockSize.setColumns(10);
		
		JLabel lblConcurrencyControlProtocol = new JLabel("Concurrency Control Protocol:");
		lblConcurrencyControlProtocol.setToolTipText("Maximum Pages in Buffer");
		GridBagConstraints gbc_lblConcurrencyControlProtocol = new GridBagConstraints();
		gbc_lblConcurrencyControlProtocol.anchor = GridBagConstraints.WEST;
		gbc_lblConcurrencyControlProtocol.insets = new Insets(0, 0, 5, 5);
		gbc_lblConcurrencyControlProtocol.gridx = 2;
		gbc_lblConcurrencyControlProtocol.gridy = 10;
		buffer.add(lblConcurrencyControlProtocol, gbc_lblConcurrencyControlProtocol);
		
		Vector<String> vector = new Vector<>();
		vector.add("Rigorous 2PL");
		//vector.add("2V2PL");
		JComboBox<String> comboBox_1 = new JComboBox<String>(vector);
		comboBox_1.setMaximumRowCount(12);
		GridBagConstraints gbc_comboBox_1 = new GridBagConstraints();
		gbc_comboBox_1.gridwidth = 10;
		gbc_comboBox_1.insets = new Insets(0, 0, 5, 5);
		gbc_comboBox_1.fill = GridBagConstraints.HORIZONTAL;
		gbc_comboBox_1.gridx = 3;
		gbc_comboBox_1.gridy = 10;
		buffer.add(comboBox_1, gbc_comboBox_1);
		
		
		
		JButton buttonStart = new JButton("Start");
		GridBagConstraints gbc_buttonStart = new GridBagConstraints();
		gbc_buttonStart.gridwidth = 4;
		gbc_buttonStart.insets = new Insets(0, 0, 5, 5);
		gbc_buttonStart.gridx = 9;
		gbc_buttonStart.gridy = 12;
		buffer.add(buttonStart, gbc_buttonStart);
		buttonStart.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent arg0) {
				if(isNumeric(textFieldbuferSize.getText(), "Buffer Size Invalid")){
					Kernel.BUFFER_SIZE = Integer.parseInt(textFieldbuferSize.getText());					
				}else{
					return;
				}
				
//				if(isNumeric(textFieldbuferSize.getText(), "Block Size Invalid")){ //TODO HERE
//					Kernel.BLOCK_SIZE = Integer.parseInt(textFieldBlockSize.getText());					
//				}else{
//					return;
//				}
				
				if(isNumeric(textFieldPort.getText(), "Port Invalid")){
					Kernel.PORT = Integer.parseInt(textFieldPort.getText());					
				}else{
					return;
				}
				
				Kernel.setBufferPolicy((String) comboBox.getSelectedItem());
				//Kernel.setBufferPolicy( new LRU());				
				
				Kernel.start();
				setVisible(false);
				
				@SuppressWarnings("unused")
				ConnectionFrame connectionFrame = ConnectionFrame.getInstance();
				
			}
		});
		
		
	}

	private static boolean isNumeric(String a,String msg) {
		try {
			int t = Integer.parseInt(a);
			if(t<1)throw new NumberFormatException();
		} catch (Exception e) {
			JOptionPane.showMessageDialog(null, msg, "Erro", JOptionPane.ERROR_MESSAGE);
			Kernel.exception(SettingsFrame.class,e);
			return false;
		}
		return true;
	}
}
