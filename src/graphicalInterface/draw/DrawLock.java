package graphicalInterface.draw;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import DBMS.transactionManager.Lock;

public class DrawLock extends JPanel{
	
	private static final long serialVersionUID = 1L;
	private JLabel idObject;
	private JLabel trasactionID;
	
	
	public DrawLock(){
		idObject = new JLabel();
		trasactionID = new JLabel();
		this.setPreferredSize(new Dimension(120, 40));
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		idObject.setHorizontalAlignment(JLabel.CENTER);
		trasactionID.setHorizontalAlignment(JLabel.CENTER);
		this.setLayout(new GridLayout(1, 2));
		this.add(idObject);
		this.add(trasactionID);
	}
	
	public void update(Lock lock){
	
		if(lock!=null){
			if(lock.getLockType()==Lock.READ_LOCK){
				idObject.setText("RL("+lock.getObjectDatabaseId()+")");
				this.setBackground(DrawPage.READ_COLOR);
			}else{
				idObject.setText("WL("+lock.getObjectDatabaseId()+")");
				this.setBackground(DrawPage.WRITE_COLOR);
			}
			trasactionID.setText("T"+lock.getTransaction().getIdT());
		}else{
			this.setBackground(Color.WHITE);
		}
	}

	public JLabel getIdObject() {
		return idObject;
	}

	public void setIdObject(JLabel idObject) {
		this.idObject = idObject;
	}

	public JLabel getTrasactionID() {
		return trasactionID;
	}

	public void setTrasactionID(JLabel trasactionID) {
		this.trasactionID = trasactionID;
	}

	
	
}
