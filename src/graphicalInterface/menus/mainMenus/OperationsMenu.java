package graphicalInterface.menus.mainMenus;

import java.awt.Color;

import java.awt.Dimension;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import graphicalInterface.Events;
import graphicalInterface.images.ImagensController;
import graphicalInterface.util.MeshLayout;

public class OperationsMenu extends JScrollPane {

	
	private static final long serialVersionUID = 1L;
	private JPanel js = new JPanel();
	private Dimension dimension = new Dimension();
	private JPanel panelArea;
	
	
	public void createButton(ImageIcon imageIcon) {
		JButton button = new JButton(imageIcon);
		button.setName(imageIcon.toString());
		button.setBorderPainted(false);
		button.setContentAreaFilled(false);
		button.setFocusPainted(false);
		button.setOpaque(false);
		js.add(button);
		button.addMouseListener(new MouseListener() {
			

		
			public void mousePressed(MouseEvent e) {
				Events.setMouseOperationIcon((ImageIcon) button.getIcon(),panelArea);
				
			}
				
			public void mouseReleased(MouseEvent e){}
			public void mouseExited(MouseEvent e){}
			public void mouseEntered(MouseEvent e){}
			public void mouseClicked(MouseEvent e) {}
		});
		/*
		 * 
		button.addActionListener(new ActionListener() {
		
			public void actionPerformed(ActionEvent e) {			
				Events.setMouseOperationIcon((ImageIcon) button.getIcon());
				
			}
		});
		 */
	}

	public OperationsMenu(JPanel area) {
		this.panelArea = area;
		js.setBackground(Color.white);
		js.setLayout(new MeshLayout(4, 1));
		JLabel label = new JLabel("Click an operation");
		label.setHorizontalAlignment(JLabel.CENTER);
		js.add(label);
		this.setViewportView(js);
		dimension.setSize(ImagensController.BUTTON_JOIN.getIconWidth(), ImagensController.BUTTON_JOIN.getIconHeight());
		this.setPreferredSize(new Dimension(10, 29));
		createButton(ImagensController.BUTTON_TABLE);
		createButton(ImagensController.BUTTON_JOIN);
		createButton(ImagensController.BUTTON_PROJECTION);
		createButton(ImagensController.BUTTON_SELECTION);
		createButton(ImagensController.BUTTON_UNION);
		createButton(ImagensController.BUTTON_INTERSECTION);
		createButton(ImagensController.BUTTON_FILTER);
		createButton(ImagensController.BUTTON_AGGREGATION);
		createButton(ImagensController.BUTTON_SORT);
		createButton(ImagensController.BUTTON_SUBPLAN);
		createButton(ImagensController.BUTTON_GROUP_RESULTS);
	}

	public Dimension getDimension() {
		return dimension;
	}

}
