package graphicalInterface.util;

import java.awt.Component;
import java.util.ArrayList;

import javax.swing.JSplitPane;


public class JSplitPaneMultiTabs extends JSplitPane {
	
	private static final long serialVersionUID = 1L;
	private ArrayList<JSplitPane> encapsulationList = new ArrayList<JSplitPane>();
	private int numberOfComponents = 1;
	private int sizeOfDivision = 6;
	
	/**
	 * This Components is based on the JSplitPane. JSplitPane is used to divide two
	 * (and only two) Components. This class intend to manipulate the JSplitPane in
	 * a way that can be placed as many Component as wanted.
	 * 
	 * @author Bode
	 *
	 */
	public JSplitPaneMultiTabs() {
		super();
		this.setLeftComponent(null);
		this.setBorder(null);
		encapsulationList.add(this);
		setAllBorders(sizeOfDivision);
	}
	 /**
     * 
     * @param comp - adds a Component to the Pane
     */
	public JSplitPane addComponent(Component comp) {
		JSplitPane capsule = new JSplitPane();	
		capsule.setRightComponent(null);
		capsule.setLeftComponent(comp);
		capsule.setDividerSize(sizeOfDivision);
		capsule.setBorder(null);
		encapsulationList.get(numberOfComponents - 1).setRightComponent(capsule);
		
		encapsulationList.add(capsule);
		numberOfComponents++;
		
		//this.fixWeights(); change it
		return capsule;
	}

	/**
     * 
     * @param orientation
     *            JSplitPane.HORIZONTAL_SPLIT - sets the orientation of the
     *            Components to horizontal alignment
     * @param orientation
     *            JSplitPane.VERTICAL_SPLIT - sets the orientation of the
     *            Components to vertical alignment
     */
	public void setAlignment(int orientation) {
		for (int i = 0; i < numberOfComponents; i++) {
			encapsulationList.get(i).setOrientation(orientation);

		}
	}

	 /**
     * 
     * @param newSize - resizes the borders of the all the Components of the Screen
     */
	public void setAllBorders(int newSize) {
		this.setDividerSize(newSize);
		for (int i = 0; i < numberOfComponents; i++) {
			encapsulationList.get(i).setDividerSize(newSize);
		}

	}
	/**
     * each Component added needs to be readapteded to the screen
     */
	private void fixWeights() {
		encapsulationList.get(0).setResizeWeight(1.0);
		for (int i = 1; i < numberOfComponents; i++) {
			double resize = (double) 1 / (double) (i + 1);
			encapsulationList.get(numberOfComponents - i - 1).setResizeWeight(resize);
		}
		encapsulationList.get(numberOfComponents - 1).setResizeWeight(0.0);
	}

	/**
	 * 
	 * @param comp
	 *            - Component to be removed
	 */
	public void removeComponent(Component comp) {
		for (int i = 0; i < numberOfComponents; i++) {
			//treats when there are just 2 elements
			if (numberOfComponents == 2) {
				if (comp == encapsulationList.get(i).getLeftComponent()) {
					System.out.println("Valor de ==2 : " + i);
					encapsulationList.remove(i);
					numberOfComponents--;
					encapsulationList.get(0).setRightComponent(null);
				}

			}else// when there are more than 2 elements but not the final element
			if (numberOfComponents > 2 && i != (numberOfComponents - 1)) {
				if (comp == encapsulationList.get(i).getLeftComponent()) {
					System.out.println("Valor de >2 : " + i);
					encapsulationList.get(i - 1).setRightComponent(encapsulationList.get(i + 1));
					encapsulationList.remove(i);
					numberOfComponents--;

				}

			}else// treats the final element
			if (i == (numberOfComponents - 1)) {
				if (comp == encapsulationList.get(i).getLeftComponent()) {
					System.out.println("Valor de i== -1 : " + i);
					encapsulationList.get(i - 1).setRightComponent(null);
					encapsulationList.remove(i);
					numberOfComponents--;

				}
			}
			
			this.fixWeights();

		}

	}
	/**
	 * 
	 * @param index - index to remove the items
	 */
	public void removeComponent(int index) {
		index = index + 1 ;
		for (int i = 1; i < numberOfComponents; i++) {
			//treats when there are just 2 elements
			if (numberOfComponents == 2) {
				if (index == i) {
					System.out.println("Valor de ==2 : " + i);
					encapsulationList.remove(i);
					numberOfComponents--;
					encapsulationList.get(0).setRightComponent(null);
				}

			}else// when there are more than 2 elements but not the final element
			if (numberOfComponents > 2 && i != (numberOfComponents - 1)) {
				if (index == i) {
					System.out.println("Valor de >2 : " + i);
					encapsulationList.get(i - 1).setRightComponent(encapsulationList.get(i + 1));
					encapsulationList.remove(i);
					numberOfComponents--;

				}

			}else// treats the final element
			if (i == (numberOfComponents - 1)) {
				if (index == i) {
					System.out.println("Valor de i== -1 : " + i);
					encapsulationList.get(i - 1).setRightComponent(null);
					encapsulationList.remove(i);
					numberOfComponents--;

				}
			}
			
			this.fixWeights();

		}

	}

	public int getNumberOfComponents() {
		return numberOfComponents;
	}

	
	
	
	
	
}