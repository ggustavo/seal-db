package graphicalInterface;


import java.awt.Cursor;

import java.awt.Image;

import java.awt.Toolkit;

import javax.swing.ImageIcon;
import javax.swing.JPanel;

import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.AggregationOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.FilterOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.GroupResultsOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.IntersectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SortOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SubplanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.UnionOperation;
import graphicalInterface.images.ImagensController;

public class Events {

	
	
	//public static JPanel panelArea;
	public static boolean clickButton = false;
	public static Class<?> classOperation;
	
	private static Toolkit toolkit = Toolkit.getDefaultToolkit();  
	private static Image image = null;
	private static java.awt.Point hotSpot = new java.awt.Point(0, 1);  
	
	
	 
	public static void setMouseOperationIcon(ImageIcon imageIcon,JPanel panelArea){
		
        if(imageIcon == ImagensController.BUTTON_JOIN){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_join.png")); 
        	classOperation = JoinOperation.class;
        	
        }else if(imageIcon == ImagensController.BUTTON_SELECTION){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_selection.png"));
        	classOperation = SelectionOperation.class;
        	
        }else  if(imageIcon == ImagensController.BUTTON_PROJECTION){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_projection.png")); 
        	classOperation = ProjectionOperation.class;
        	
        }else if(imageIcon == ImagensController.BUTTON_TABLE){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_table.png")); 
        	classOperation = TableOperation.class;
        }else if(imageIcon == ImagensController.BUTTON_SORT){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_sort.png")); 
        	classOperation = SortOperation.class;
        }else if(imageIcon == ImagensController.BUTTON_AGGREGATION){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_aggregation.png")); 
        	classOperation = AggregationOperation.class;
        }else if(imageIcon == ImagensController.BUTTON_UNION){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_union.png")); 
        	classOperation = UnionOperation.class;
        }else if(imageIcon == ImagensController.BUTTON_SUBPLAN){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_subplan.png")); 
        	classOperation = SubplanOperation.class;
        }else if(imageIcon == ImagensController.BUTTON_GROUP_RESULTS){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_materialize_results.png")); 
        	classOperation = GroupResultsOperation.class;
        }else if(imageIcon == ImagensController.BUTTON_FILTER){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_filter.png")); 
        	classOperation = FilterOperation.class;
        }else if(imageIcon == ImagensController.BUTTON_INTERSECTION){
        	
        	image = toolkit.getImage(ImagensController.class.getResource("drag_intersection.png")); 
        	classOperation = IntersectionOperation.class;
        }
       
       
        
        Cursor cursor = toolkit.createCustomCursor(image, hotSpot, "Pencil");  
        panelArea.setCursor(cursor);
        
        
		clickButton = true;
	}
	
	public static void setMouseNormalIcon(JPanel panelArea){
		panelArea.setCursor( new Cursor( Cursor.DEFAULT_CURSOR));
		//panelArea.setCursor( Cursor.getPredefinedCursor( Cursor.DEFAULT_CURSOR));
		clickButton = false;
		
	}

}
