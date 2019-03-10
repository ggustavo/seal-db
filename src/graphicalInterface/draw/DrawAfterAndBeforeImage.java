package graphicalInterface.draw;

import javax.swing.JFrame;

import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
import DBMS.fileManager.dataAcessManager.file.data.FileTuple;
import DBMS.fileManager.dataAcessManager.file.log.FileRecord;
import DBMS.queryProcessing.TupleManipulate;
import graphicalInterface.images.ImagensController;

import java.awt.Color;
import java.awt.GridLayout;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import java.awt.Component;
import java.awt.Dimension;

public class DrawAfterAndBeforeImage extends JFrame{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public DrawAfterAndBeforeImage(FileRecord fileRecord){
		
		super();
		setLocationRelativeTo(null);
		setIconImage(ImagensController.FRAME_ICON_DATABASE_EYE);
		setTitle("Log Record Details, LSN: " + fileRecord.getLSN() + ", Transaction ID: " + fileRecord.getTransactionId());
		getContentPane().setLayout(new GridLayout(1, 0, 0, 0));

		JPanel panel_before = new JPanel();
		panel_before.setBackground(Color.WHITE);
		panel_before.setLayout(new BoxLayout(panel_before, BoxLayout.Y_AXIS));
		getContentPane().add(panel_before);


		JPanel panel_after = new JPanel();
		panel_after.setBackground(Color.WHITE);
		panel_after.setLayout(new BoxLayout(panel_after, BoxLayout.Y_AXIS));
		getContentPane().add(panel_after);
		
		
		JLabel lblNewLabel_1 = new JLabel("After Image");
		lblNewLabel_1.setPreferredSize(new Dimension(60, 50));
		lblNewLabel_1.setAlignmentX(Component.CENTER_ALIGNMENT);
		panel_after.add(lblNewLabel_1);
		
		JLabel lblNewLabel = new JLabel("Before Image");
		lblNewLabel.setPreferredSize(new Dimension(60, 50));
		lblNewLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
		panel_before.add(lblNewLabel);
		
		setTable(panel_after, new FileBlock(fileRecord.getAfterImage()));
		setTable(panel_before, new FileBlock(fileRecord.getBeforeImage()));
		
		this.setBackground(Color.white);
		this.pack();
		this.setVisible(true);
	}
	
	public void setTable(JPanel panel, FileBlock fileBlock){

		if(fileBlock.getStatus()==-1){
			return;
		}
		ArrayList<FileTuple> ftuples = fileBlock.readTuplesArray();
		ArrayList<TupleManipulate> tupleManipulates = new ArrayList<>();
		for (FileTuple fileTuple : ftuples) {
			tupleManipulates.add(new TupleManipulate(fileTuple.getTupleID(),fileTuple.getData()));
		}
		
		int columnCount = tupleManipulates.get(0).getData().length;
	
		AbstractTableModel model = new AbstractTableModel() {
			private static final long serialVersionUID = 1L;

			@Override
			public Object getValueAt(int rowIndex, int columnIndex) {
				
				if(rowIndex < tupleManipulates.size()){
					
					TupleManipulate t = tupleManipulates.get(rowIndex);
					
					return t.getColunmData(columnIndex);
					
				}
				
				return null;
			}
			
			@Override
			public int getRowCount() {
				return tupleManipulates.size();
			}
			
			@Override
			public int getColumnCount() {
				return columnCount;
			}
		};
		
			
		 
		JTable table = new JTable(model);
		table.setFillsViewportHeight(true);
		table.setTableHeader(null);
		JScrollPane scrollPane = new JScrollPane(table);
		panel.add(scrollPane);
     
	}
	
}
