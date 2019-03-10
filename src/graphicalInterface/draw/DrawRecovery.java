package graphicalInterface.draw;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JButton;
import javax.swing.JFormattedTextField;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingConstants;

import DBMS.Kernel;
import DBMS.fileManager.dataAcessManager.file.log.FileRecord;
import DBMS.recoveryManager.RecoveryManagerListener;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JTextField;
import javax.swing.border.LineBorder;
import javax.swing.text.MaskFormatter;

public class DrawRecovery extends JPanel{

	private static final long serialVersionUID = 1L;

	
	private JPanel realTimeList;
	private JPanel searchList;
	private JTextField transaction;
	private JTextField lsn1;
	private JTextField lsn2;
	private JFormattedTextField date1;
	private JFormattedTextField date2;

	
	public DrawRecovery(){
		setLayout(new GridLayout(0, 1, 0, 0));
		
		JSplitPane splitPane = new JSplitPane();
		splitPane.setResizeWeight(0.40);
		add(splitPane);
		
		JPanel realTimePanel = new JPanel();
		realTimePanel.setBackground(Color.WHITE);
		splitPane.setLeftComponent(realTimePanel);
		realTimePanel.setLayout(new BoxLayout(realTimePanel, BoxLayout.Y_AXIS));
		
		JLabel lblrTimeLogRecord = new JLabel("Real-Time Log Records");
		lblrTimeLogRecord.setPreferredSize(new Dimension(60, 50));
		realTimePanel.add(lblrTimeLogRecord);
		lblrTimeLogRecord.setBorder(null);
		lblrTimeLogRecord.setHorizontalTextPosition(SwingConstants.CENTER);
		lblrTimeLogRecord.setAlignmentX(Component.CENTER_ALIGNMENT);
		lblrTimeLogRecord.setHorizontalAlignment(SwingConstants.CENTER);
		
		JScrollPane scrollPane = new JScrollPane();
		realTimePanel.add(scrollPane);
		
		JPanel logMine = new JPanel();
		logMine.setBackground(Color.WHITE);
		splitPane.setRightComponent(logMine);		
		logMine.setLayout(new BoxLayout(logMine, BoxLayout.Y_AXIS));
		
		JLabel lblSearch = new JLabel("Search");
		lblSearch.setPreferredSize(new Dimension(60, 50));
		lblSearch.setAlignmentX(Component.CENTER_ALIGNMENT);
		logMine.add(lblSearch);
		
		JPanel searchPanel = new JPanel();
		searchPanel.setBorder(new LineBorder(new Color(0, 0, 0)));
		logMine.add(searchPanel);
		searchPanel.setLayout(new BoxLayout(searchPanel, BoxLayout.Y_AXIS));
		
		JPanel paneltop = new JPanel();
		searchPanel.add(paneltop);
		
		JLabel lsn_label = new JLabel("  LSN: From - To");
		
		JButton searchLSN = new JButton("Search");
		searchLSN.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(validInteger(lsn1.getText()) && validInteger(lsn2.getText())){
					List<FileRecord> result = Kernel.getRecoveryManager().findByLSN(Integer.parseInt(lsn1.getText()), Integer.parseInt(lsn2.getText()));
					if(result.isEmpty()){
						JOptionPane.showMessageDialog(DrawRecovery.this, "No results found","Error", JOptionPane.ERROR_MESSAGE);
						return;
					}
					searchList.removeAll();
					for (FileRecord f : result) {
						DrawRecordLog drawRecordLog = new DrawRecordLog();
						drawRecordLog.bind(f);
						searchList.add(drawRecordLog);
					}
					searchList.revalidate();
					searchList.repaint();					
				}
			}
		});
		
		JLabel date_label = new JLabel("  Date: From - To");
		
		date1 = new JFormattedTextField();
		
		
		JButton searchDate = new JButton("Search");
		searchDate.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(validString(date1.getText()) && validString(date2.getText())){
					
					
					SimpleDateFormat dt = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
					try {
						Date dd1 = dt.parse(date1.getText());
						Date dd2 = dt.parse(date2.getText());
						List<FileRecord> result = Kernel.getRecoveryManager().findByDate(dd1, dd2);
						if(result.isEmpty()){
							JOptionPane.showMessageDialog(DrawRecovery.this, "No results found","Error", JOptionPane.ERROR_MESSAGE);
							return;
						}
						searchList.removeAll();
						for (FileRecord f : result) {
							DrawRecordLog drawRecordLog = new DrawRecordLog();
							drawRecordLog.bind(f);
							searchList.add(drawRecordLog);
						}
						searchList.revalidate();
						searchList.repaint();	
					} catch (ParseException e1) {
						
						Kernel.exception(this.getClass(),e1);
					}
								
				}
			}
		});
		
		JLabel transaction_label = new JLabel("  Transaction ID:");
		
		JButton searchTransaction = new JButton("Search");
		searchTransaction.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(validInteger(transaction.getText())){
					List<FileRecord> result = Kernel.getRecoveryManager().findByTransaction(Integer.parseInt(transaction.getText()));
					if(result.isEmpty()){
						JOptionPane.showMessageDialog(DrawRecovery.this, "No results found","Error", JOptionPane.ERROR_MESSAGE);
						return;
					}
					searchList.removeAll();
					for (FileRecord f : result) {
						DrawRecordLog drawRecordLog = new DrawRecordLog();
						drawRecordLog.bind(f);
						searchList.add(drawRecordLog);
					}
					searchList.revalidate();
					searchList.repaint();					
				}
				
			}
		});
		
		date2 = new JFormattedTextField();
		try {
			MaskFormatter maskData1 = new MaskFormatter("####/##/## ##:##:##");
			maskData1.install(date1);
			MaskFormatter maskData2 = new MaskFormatter("####/##/## ##:##:##");
			maskData2.install(date2);
			
			Date date = new Date();
			SimpleDateFormat dt = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
			
			Calendar cal = Calendar.getInstance();
			// remove next line if you're always using the current time.
			cal.setTime(new Date());
			cal.add(Calendar.MINUTE, -1);
			Date oneMinuteBack = cal.getTime();
			date1.setText(dt.format(oneMinuteBack));
			date2.setText(dt.format(date));
		
		} catch (ParseException e1) {
			Kernel.exception(this.getClass(),e1);
		}
		transaction = new JTextField();
		transaction.setColumns(10);
		
		lsn1 = new JTextField();
		lsn1.setColumns(10);
		
		lsn2 = new JTextField();
		lsn2.setColumns(10);
		GroupLayout gl_paneltop = new GroupLayout(paneltop);
		gl_paneltop.setHorizontalGroup(
			gl_paneltop.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_paneltop.createSequentialGroup()
					.addGroup(gl_paneltop.createParallelGroup(Alignment.LEADING)
						.addComponent(transaction_label)
						.addComponent(date_label)
						.addComponent(lsn_label))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(gl_paneltop.createParallelGroup(Alignment.LEADING, false)
						.addComponent(transaction, 0, 0, Short.MAX_VALUE)
						.addComponent(lsn1, GroupLayout.DEFAULT_SIZE, 120, Short.MAX_VALUE)
						.addComponent(date1))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(gl_paneltop.createParallelGroup(Alignment.LEADING, false)
						.addComponent(date2)
						.addComponent(lsn2, GroupLayout.DEFAULT_SIZE, 116, Short.MAX_VALUE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(gl_paneltop.createParallelGroup(Alignment.TRAILING)
						.addGroup(gl_paneltop.createParallelGroup(Alignment.LEADING, false)
							.addComponent(searchDate, 0, 0, Short.MAX_VALUE)
							.addComponent(searchTransaction, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
						.addComponent(searchLSN))
					.addContainerGap(12, Short.MAX_VALUE))
		);
		gl_paneltop.setVerticalGroup(
			gl_paneltop.createParallelGroup(Alignment.LEADING)
				.addGroup(gl_paneltop.createSequentialGroup()
					.addGap(30)
					.addGroup(gl_paneltop.createParallelGroup(Alignment.LEADING)
						.addComponent(lsn_label)
						.addComponent(searchLSN)
						.addGroup(gl_paneltop.createParallelGroup(Alignment.BASELINE)
							.addComponent(lsn1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addComponent(lsn2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
					.addGap(10)
					.addGroup(gl_paneltop.createParallelGroup(Alignment.LEADING)
						.addComponent(date_label)
						.addGroup(gl_paneltop.createSequentialGroup()
							.addGap(1)
							.addGroup(gl_paneltop.createParallelGroup(Alignment.BASELINE)
								.addComponent(date1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(date2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
						.addComponent(searchDate))
					.addGap(10)
					.addGroup(gl_paneltop.createParallelGroup(Alignment.LEADING)
						.addComponent(transaction_label)
						.addComponent(searchTransaction)
						.addComponent(transaction, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(10))
		);
		paneltop.setLayout(gl_paneltop);
		
		JScrollPane scrollPane_1 = new JScrollPane();
		searchPanel.add(scrollPane_1);
		
		searchList = new JPanel();
		searchList.setLayout(new GridLayout(300, 1));
		scrollPane_1.setViewportView(searchList);
		
		
		JLabel lblClearList = new JLabel("Clear List");
		lblClearList.setPreferredSize(new Dimension(60, 50));
		lblClearList.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				realTimeList.removeAll();
				realTimeList.revalidate();
				realTimeList.repaint();
			}
		});
		lblClearList.setAlignmentX(Component.CENTER_ALIGNMENT);
		realTimePanel.add(lblClearList);
		
		configList(scrollPane);
	}
	
	private boolean validString(String s){
		if(s!=null && !s.isEmpty()){
			return true;
		}
		JOptionPane.showMessageDialog(this, s + " <- Invalid Value", "Error", JOptionPane.ERROR_MESSAGE);
		return false;
	}
	private boolean validInteger(String s){
		if(s!=null && !s.isEmpty()){
			try{
				Integer.parseInt(s);				
			}catch (Exception e) {
				JOptionPane.showMessageDialog(this, s + " <- Invalid Value", "Error", JOptionPane.ERROR_MESSAGE);
				return false;
			}
			return true;
		}
		return false;
	}
	
	private void configList(JScrollPane scrollPane){
		realTimeList = new JPanel();
		realTimeList.setLayout(new GridLayout(300, 1));
		scrollPane.setViewportView(realTimeList);
		
		Kernel.getRecoveryManager().setRecoveryManagerListen(new RecoveryManagerListener() {
			
			@Override
			public void newRecord(FileRecord fileRecord) {
				DrawRecordLog drawRecordLog = new DrawRecordLog();
				drawRecordLog.bind(fileRecord);
				realTimeList.add(drawRecordLog);
				realTimeList.revalidate();
				realTimeList.repaint();
			}
		});
	}
}
