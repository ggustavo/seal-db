package graphicalInterface.draw;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.text.SimpleDateFormat;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import DBMS.fileManager.dataAcessManager.file.log.FileRecord;

public class DrawRecordLog extends JPanel{
	private static final long serialVersionUID = 1L;
	
	private final static Color BLUE = new Color(102, 178, 255);
	private final static Color RED = new Color(255, 60, 60);
	
	private JLabel lsn;
	private JLabel transaction;
	private JLabel type;
	private JLabel date;
	
	private SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	private FileRecord fileRecord;
	
	public DrawRecordLog(){
		this.setPreferredSize(new Dimension(45, 45));
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		this.setBackground(Color.white);
		this.setLayout(new GridLayout(2, 3));
		lsn = new JLabel();
		transaction = new JLabel();
		type = new JLabel();
		date = new JLabel();
		this.add(lsn);
		this.add(type);
		this.add(transaction);
		this.add(date);
		this.addMouseListener(new MouseListener() {
			
			@Override
			public void mouseReleased(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mousePressed(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseExited(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseEntered(MouseEvent e) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void mouseClicked(MouseEvent e) {
				if(fileRecord!=null){
					if(fileRecord.getRecordType() == FileRecord.UPDATE_LOG_RECORD_TYPE){
						new DrawAfterAndBeforeImage(fileRecord);						
					}
				}
				
			}
		});
		
	}
	
	public void bind(FileRecord fileRecord){
		
		lsn.setText("   LSN: " + fileRecord.getLSN());
		transaction.setText("   Transaction ID: T"+fileRecord.getTransactionId());
		date.setText(dt.format(fileRecord.getDate()));
		switch (fileRecord.getRecordType()) {
		case FileRecord.ABORT_RECORD_TYPE:
			type.setText("Abort");
			this.setBackground(RED);
			break;
		case FileRecord.COMMIT_RECORD_TYPE:
			type.setText("Commit");
			this.setBackground(Color.GREEN);
			break;
			
		case FileRecord.UPDATE_LOG_RECORD_TYPE:
			this.fileRecord = fileRecord;
			String id = fileRecord.getSchemaID() + "-"+fileRecord.getTableID()+"-"+fileRecord.getBlockID()+"-"+fileRecord.getTupleID();
			type.setText("Write on Object ID: " + id);
			this.setBackground(Color.white);
			break;
		case FileRecord.CHECKPOINT_RECORD_TYPE:
			this.setBackground(BLUE);
			transaction.setText("");
			type.setText("Checkpoint");
			break;	
			
		case FileRecord.BENGIN_RECORD_TYPE:
			type.setText("Begin");
			this.setBackground(Color.YELLOW);
			break;	
			
		default:
			break;
		}
		
	}
	
}
