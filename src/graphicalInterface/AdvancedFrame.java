package graphicalInterface;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.SwingConstants;

import graphicalInterface.draw.DrawIndex;
import graphicalInterface.draw.DrawNodesGraph;
import graphicalInterface.images.ImagensController;

public class AdvancedFrame extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public AdvancedFrame() {
		setTitle("Advanced Tools");
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setIconImage(ImagensController.FRAME_ICON_SETTINGS);
		setBounds(100, 100, 300, 217);
	
		getContentPane().setBackground(Color.WHITE);
		setBackground(Color.WHITE);
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[] {30, 200, 0};
		gridBagLayout.rowHeights = new int[]{0, 0, 0, 0, 0};
		gridBagLayout.columnWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		getContentPane().setLayout(gridBagLayout);
		
		JLabel label_1 = new JLabel("   Nodes Connections (Tests)");
		label_1.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {

				if (nodeConnectionFrame!=null && nodeConnectionFrame.isVisible()) {
					nodeConnectionFrame.setVisible(false);
				} else {
					showNodeConnectionFrame();
				}
			}
		});
		
		JLabel label_2 = new JLabel("                  ");
		GridBagConstraints gbc_label_2 = new GridBagConstraints();
		gbc_label_2.insets = new Insets(0, 0, 5, 0);
		gbc_label_2.gridx = 1;
		gbc_label_2.gridy = 0;
		getContentPane().add(label_2, gbc_label_2);
		label_1.setIcon(new ImageIcon(AdvancedFrame.class.getResource("/graphicalInterface/images/distributed.png")));
		label_1.setHorizontalAlignment(SwingConstants.LEFT);
		GridBagConstraints gbc_label_1 = new GridBagConstraints();
		gbc_label_1.anchor = GridBagConstraints.WEST;
		gbc_label_1.insets = new Insets(0, 0, 5, 0);
		gbc_label_1.fill = GridBagConstraints.VERTICAL;
		gbc_label_1.gridx = 1;
		gbc_label_1.gridy = 1;
		getContentPane().add(label_1, gbc_label_1);
		
		JLabel label = new JLabel("   Index View (Tests)");
		label.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				DrawIndex.open();
			}
		});
		label.setIcon(new ImageIcon(AdvancedFrame.class.getResource("/graphicalInterface/images/index_view.png")));
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.anchor = GridBagConstraints.WEST;
		gbc_label.insets = new Insets(0, 0, 5, 0);
		gbc_label.fill = GridBagConstraints.VERTICAL;
		gbc_label.gridx = 1;
		gbc_label.gridy = 2;
		getContentPane().add(label, gbc_label);
	}
	
	private JFrame nodeConnectionFrame;
	
	public void showNodeConnectionFrame() {
		getNodeConnectionFrame();
		nodeConnectionFrame.setVisible(true);
	}
	
	public JFrame getNodeConnectionFrame() {
		
		if(nodeConnectionFrame == null){
			nodeConnectionFrame = DrawNodesGraph.open();
		}
		return nodeConnectionFrame;
	}
	
	private static AdvancedFrame advancedFrame;
	public static void open() {
		if(advancedFrame==null) {
			advancedFrame = new AdvancedFrame();
		}
		advancedFrame.setVisible(true);
	}
}
