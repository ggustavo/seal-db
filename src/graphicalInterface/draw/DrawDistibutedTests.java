package graphicalInterface.draw;

import javax.swing.JFrame;
import java.awt.GridBagLayout;
import java.awt.BorderLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import java.awt.Color;
import javax.swing.JLabel;
import javax.swing.JTextField;
import java.awt.GridLayout;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import javax.swing.JButton;
import javax.swing.border.LineBorder;

import DBMS.Kernel;
import DBMS.distributed.DistributedTransactionManagerController;
import DBMS.distributed.ResourceManagerConnection;
import graphicalInterface.util.SQLHighlighter;

import javax.swing.JTextPane;
import java.awt.event.ActionListener;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.awt.event.ActionEvent;
import javax.swing.border.EmptyBorder;

public class DrawDistibutedTests extends JFrame{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JTextField host;
	private JTextField port;
	private JTextField schema;
	
	
	private JTextPane textSQL;
	
	public JPanel graph;
	public JPanel node;
	private JLabel nodeName;
	
	
	private HashMap<String, ResourceManagerConnection> nodes;
	
	public DrawDistibutedTests() {
		getContentPane().setLayout(new BorderLayout(0, 0));
		nodes = new HashMap<>();
		JPanel menu = new JPanel();
		menu.setBorder(new EmptyBorder(20, 20, 20, 20));
		menu.setBackground(Color.WHITE);
		getContentPane().add(menu, BorderLayout.WEST);
		menu.setLayout(new GridLayout(0, 1, 0, 0));
		
		JPanel panel = new JPanel();
		menu.add(panel);
		panel.setBackground(Color.WHITE);
		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0, 0, 0, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel.columnWeights = new double[]{0.0, 1.0, 1.0, 0.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel.setLayout(gbl_panel);
		
		JLabel lblIp = new JLabel("Host:");
		GridBagConstraints gbc_lblIp = new GridBagConstraints();
		gbc_lblIp.anchor = GridBagConstraints.WEST;
		gbc_lblIp.insets = new Insets(0, 0, 5, 5);
		gbc_lblIp.gridx = 2;
		gbc_lblIp.gridy = 1;
		panel.add(lblIp, gbc_lblIp);
		
		host = new JTextField();
		host.setText("localhost");
		GridBagConstraints gbc_host = new GridBagConstraints();
		gbc_host.fill = GridBagConstraints.HORIZONTAL;
		gbc_host.insets = new Insets(0, 0, 5, 5);
		gbc_host.gridx = 2;
		gbc_host.gridy = 2;
		panel.add(host, gbc_host);
		host.setColumns(10);
		
		JLabel lblPort = new JLabel("Port:");
		GridBagConstraints gbc_lblPort = new GridBagConstraints();
		gbc_lblPort.anchor = GridBagConstraints.WEST;
		gbc_lblPort.insets = new Insets(0, 0, 5, 5);
		gbc_lblPort.gridx = 2;
		gbc_lblPort.gridy = 3;
		panel.add(lblPort, gbc_lblPort);
		
		port = new JTextField();
		port.setText("3001");
		GridBagConstraints gbc_port = new GridBagConstraints();
		gbc_port.insets = new Insets(0, 0, 5, 5);
		gbc_port.fill = GridBagConstraints.HORIZONTAL;
		gbc_port.gridx = 2;
		gbc_port.gridy = 4;
		panel.add(port, gbc_port);
		port.setColumns(10);
		
		JButton btnNewConnection = new JButton("New Connection");
		btnNewConnection.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				
				DistributedTransactionManagerController TM = Kernel.getTransactionManager(); 
				
				try {
					ResourceManagerConnection nodeA = TM.register(InetAddress.getByName(host.getText()), Integer.parseInt(port.getText()), schema.getText(), "admin", "admin");
					nodes.put(host.getText()+":"+port.getText(),nodeA);
				
				
				
				
				} catch (UnknownHostException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
			}
		});
		
		JLabel lblNewLabel = new JLabel("Schema:");
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.anchor = GridBagConstraints.WEST;
		gbc_lblNewLabel.insets = new Insets(0, 0, 5, 5);
		gbc_lblNewLabel.gridx = 2;
		gbc_lblNewLabel.gridy = 5;
		panel.add(lblNewLabel, gbc_lblNewLabel);
		
		schema = new JTextField();
		schema.setText("company");
		GridBagConstraints gbc_schema = new GridBagConstraints();
		gbc_schema.insets = new Insets(0, 0, 5, 5);
		gbc_schema.fill = GridBagConstraints.HORIZONTAL;
		gbc_schema.gridx = 2;
		gbc_schema.gridy = 6;
		panel.add(schema, gbc_schema);
		schema.setColumns(10);
		GridBagConstraints gbc_btnNewConnection = new GridBagConstraints();
		gbc_btnNewConnection.insets = new Insets(0, 0, 0, 5);
		gbc_btnNewConnection.gridx = 2;
		gbc_btnNewConnection.gridy = 7;
		panel.add(btnNewConnection, gbc_btnNewConnection);
		
		node = new JPanel();
		node.setBackground(Color.WHITE);
		menu.add(node);
		GridBagLayout gbl_node = new GridBagLayout();
		gbl_node.columnWidths = new int[]{0, 0, 0, 0};
		gbl_node.rowHeights = new int[]{0, 0, 0, 0, 0, 0};
		gbl_node.columnWeights = new double[]{0.0, 1.0, 0.0, Double.MIN_VALUE};
		gbl_node.rowWeights = new double[]{0.0, 0.0, 1.0, 0.0, 0.0, Double.MIN_VALUE};
		node.setLayout(gbl_node);
		
		nodeName = new JLabel("Node:");
		GridBagConstraints gbc_nodeName = new GridBagConstraints();
		gbc_nodeName.insets = new Insets(0, 0, 5, 5);
		gbc_nodeName.gridx = 1;
		gbc_nodeName.gridy = 1;
		node.add(nodeName, gbc_nodeName);
		
		JScrollPane scrollPane = new JScrollPane();
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.insets = new Insets(0, 0, 5, 5);
		gbc_scrollPane.gridx = 1;
		gbc_scrollPane.gridy = 2;
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		node.add(scrollPane, gbc_scrollPane);
		
		textSQL = new JTextPane(new SQLHighlighter());
		textSQL.setText("select * from employee");
		scrollPane.setViewportView(textSQL);
		
		JButton btnSendFragment = new JButton("Send Fragment");
		GridBagConstraints gbc_btnSendFragment = new GridBagConstraints();
		gbc_btnSendFragment.insets = new Insets(0, 0, 5, 5);
		gbc_btnSendFragment.gridx = 1;
		gbc_btnSendFragment.gridy = 3;
		node.add(btnSendFragment, gbc_btnSendFragment);
		
		graph = new JPanel();
		graph.setBorder(new LineBorder(new Color(0, 0, 0)));
		graph.setBackground(Color.GRAY);
		getContentPane().add(graph, BorderLayout.CENTER);
		graph.setLayout(new GridLayout(1, 0, 0, 0));
	}

	public void hideNode() {
		node.setVisible(false);
	}
	
	public void showNode(String nodeName) {
		node.setVisible(true);
		this.nodeName.setText("Node: " + nodeName);
	}
	
}
