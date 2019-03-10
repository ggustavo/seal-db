package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import DBMS.Kernel;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.parse.ParseVisitor;
import DBMS.queryProcessing.queryEngine.Plan;
import DBMS.queryProcessing.queryEngine.planEngine.Condition;
import DBMS.queryProcessing.queryEngine.planEngine.joinAlgorithms.BlockNestedLoopJoin;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.AbstractPlanOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.AggregationOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.JoinOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.ProjectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SelectionOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.SortOperation;
import DBMS.queryProcessing.queryEngine.planEngine.planOperations.selectCommands.TableOperation;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;

public class SelectStatementParse implements StatementParse{

	public SelectStatementParse() {
		// TODO Auto-generated constructor stub
	}
	
	
	private LinkedList<TableNode> tablesNodes = new LinkedList<>();
	private LinkedList<JoinEdge> joinEdges = new LinkedList<>();
	private Plan plan = new Plan(null);
	

	class JoinEdge{
		
		TableNode right;
		TableNode left;
		LinkedList<Condition> aov;
		boolean used = false;
		boolean subSelelect = false;
		public JoinEdge(){
			aov = new LinkedList<Condition>();
			joinEdges.add(this);
		}
		
		public void reverse(){
			TableNode s = right;
			right = left;
			left = s;
			for (Condition r : aov) {
				r.reverse();
			}
		}
		
		public String toString() {
			return "JoinEdge [ left=" + left.DBTable.getName() +" right=" + right.DBTable.getName() + ", aov=" + aov.toString() + "]";
		}
		
		
	}
	
	class TableNode{
		LinkedList<String> alias;
		ITable DBTable;
		LinkedList<JoinEdge> joinEdge;
	
		TableOperation tableOP;
		SelectionOperation selectOP;
		SortOperation sortOP;
		
		Plan subPlan;
		
		
		public TableNode(Plan subPlan) {
			this.subPlan = subPlan;
		}
		
		
		public TableNode(String alias_S, ITable dB_Table) {
			for (TableNode tableNode : tablesNodes) {
				if(tableNode.DBTable == dB_Table ){
					if(alias_S != null)tableNode.alias.add(alias_S.trim());
					return;
				}
			}
			alias = new LinkedList<String>();
			if(alias_S != null)alias.add(alias_S.trim());
			DBTable = dB_Table;
			tablesNodes.add(this);
			
			tableOP = new TableOperation();
			tableOP.setPlan(plan);
			tableOP.setResultLeft(DBTable);
		}
		
		public void addSelection(Condition aov){
			if(selectOP==null){
				selectOP = new SelectionOperation();
				selectOP.setPlan(plan);
				selectOP.setLeft(tableOP);
				tableOP.setFather(selectOP);
				
			}
			selectOP.getAttributesOperatorsValues().add(aov);
		}
		
		public void addSort(String column ){
			if(sortOP == null){
				sortOP = new SortOperation();
				sortOP.setPlan(plan);
				

				AbstractPlanOperation father = tableOP.getFather();
				sortOP.setFather(father);
				
				if(father instanceof JoinOperation){
					
					if(father.getLeft() == tableOP){
						father.setLeft(sortOP);
					}else{
						father.setRight(sortOP);
					}
					
				}else{
					father.setLeft(sortOP);
				}
				sortOP.setLeft(tableOP);
				tableOP.setFather(sortOP);
				sortOP.setColumnSorted(new LinkedList<String>());
				sortOP.getColumnSorted().add(column);				
			}else{
				if(!sortOP.getColumnSorted().contains(column)){
					sortOP.getColumnSorted().add(column);								
				}
			}
		}
		
	}
	
	//private  ISchema schema;
	
	public Plan parse(Statement statement , ISchema schema)throws SQLException{
		//this.schema = schema;
		Select select = (Select) statement;

		tablesFinder(select, schema);
		
		//for (TableNode tableNode : tablesNodes) {
		//	LogError.save(this.getClass(),tableNode.DB_Table.getName() + " alias: " + Arrays.toString(tableNode.alias.toArray()));
		//}

		whereParse(select);
		
		
       if(!joinEdges.isEmpty()){
    	   LinkedList<TableOperation> usedTables = new LinkedList<TableOperation>();
    	   
    	   JoinEdge join = findJoin();
    	  
    	   JoinOperation op = new JoinOperation();
   		   op.setJoinAlgorithm(new BlockNestedLoopJoin());
   		   op.setAttributesOperatorsValues(join.aov);
   		   plan.addOperation(op);
   		  
   		   if(join.subSelelect) {
   			  join.left.subPlan.getRoot().setPlan(plan);
   			  plan.addOperationDown(true, op, join.left.subPlan.getRoot());
  			  plan.addOperationDown(false, op, join.right.selectOP != null ? join.right.selectOP : join.right.tableOP);  
   		   }else {
   			   plan.addOperationDown(true, op, join.left.selectOP != null ? join.left.selectOP : join.left.tableOP);
   			   plan.addOperationDown(false, op, join.right.selectOP != null ? join.right.selectOP : join.right.tableOP);   			   
   		   }
   		   
		   join.used = true;
		   usedTables.add(join.left.tableOP);
		   usedTables.add(join.right.tableOP);
		   
		   while((join = findJoinNotUsed()) != null){
	        	//LogError.save(this.getClass(),join);
			   if(usedTables.contains(join.left.tableOP)){
	        		addJoinInPlan(join);
	        		usedTables.add(join.right.tableOP);
	        		join.used = true;
	        	}else 
	        	if(usedTables.contains(join.right.tableOP)){
	        		join.reverse();
	        		addJoinInPlan(join);
	        		usedTables.add(join.right.tableOP);
	        		join.used = true;
	        	}else {
	        		Kernel.log(this.getClass(),"There is a disconnected join",Level.SEVERE);
	        		break;
	        	}
	        }
       }  
		
		if (tablesNodes.size() == 0)
			throw new SQLException("[ERR0] No identified table");

		if (plan.getRoot() == null) {
			TableNode t = tablesNodes.get(0);
			plan.addOperation(t.selectOP != null ? t.selectOP : t.tableOP); // TODO:
		}
       
       	boolean haveProjFunc = haveProjectionFunctions(select);
       	

		SelectBody e = select.getSelectBody();
		
		if(e instanceof PlainSelect) {
			//System.out.println(((PlainSelect) e).getSelectItems());
			PlainSelect plainSelect = (PlainSelect) e;

		  	if(plainSelect.getGroupByColumnReferences() != null || haveProjFunc ) {
	       		aggregationParse(plainSelect,haveProjFunc);
	       		simpleProjectionParse();
	       		
	       	}else {
	       		projectionParse(select);       		
	       	}
			
		  	
		  	distictParse(plainSelect);
			ordeByParse(plainSelect);
		}
       
       				
		
		return plan;
	}
	
	private void distictParse(PlainSelect plainSelect) {
			
			if(plainSelect.getDistinct() != null) {
				SortOperation sortOperation = new SortOperation();
				sortOperation.setColumnSorted(plan.getRoot().getPossiblesColumnNames());
				sortOperation.setPlan(plan);
				
				AggregationOperation aggregationOperation = new AggregationOperation();
				aggregationOperation.setPlan(plan);
				for (String column : plan.getRoot().getPossiblesColumnNames()) {
					aggregationOperation.getAttributesOperatorsValues().
					add(new Condition(column, AggregationOperation.GROUPING, null));
				}
			
				plan.addOperation(plan.getRoot().getLeft(),sortOperation);	
				plan.addOperation(plan.getRoot().getLeft(),aggregationOperation);	
			}
	}
	
	private void aggregationParse(PlainSelect select, boolean havefunc) throws SQLException {
		
		if(select.getSelectItems().get(0).toString().trim().equals("*")) {
			throw new SQLException("SQL Not supported");
		}
		
		if (select.getGroupByColumnReferences() == null && havefunc) {
			String error = "";
			AggregationOperation aggregation = new AggregationOperation();
			aggregation.setPlan(plan);
			
			for (int i = 0; i < select.getSelectItems().size(); i++) {
				
				SelectExpressionItem item = (SelectExpressionItem) select.getSelectItems().get(i);
			
				if(item.getExpression() instanceof Function) {
					
					Function f = (Function) item.getExpression();
					String column = null;
					
					if(f.getParameters() != null) {
						column =  f.getParameters().getExpressions().get(0).toString().trim();
					}else {
						throw new SQLException("The Plan Generator does not support the function(*) insert the name of the column ");
					}
					
					String function = f.getName().trim();
					TableNode node = findTableByColumn(column);
					if(node == null){
						throw new SQLException("Invalid aggregation column: " + column);
					}
					aggregation.getAttributesOperatorsValues()
							.add(new Condition(removeStringAlias(column), function.toUpperCase(), null));

				} else {
					error += item.toString() + ",";
				}
			}
			if (!error.isEmpty()) {
				throw new SQLException("[ERR0] The columns [ "+error+" ] are not valid functions or are not present in a group by clause");
			}
			plan.addOperation(plan.getRoot(),aggregation);
		}else {
			
			AggregationOperation aggregation = new AggregationOperation();
			aggregation.setPlan(plan);	
			
			for (int i = 0; i < select.getSelectItems().size(); i++) {
				
				SelectExpressionItem item = (SelectExpressionItem) select.getSelectItems().get(i);
			
				if(item.getExpression() instanceof Function) {
					Function f = (Function) item.getExpression();
					String column = null;
					
					if(f.getParameters() != null) {
						column =  f.getParameters().getExpressions().get(0).toString().trim();
					}else {
						throw new SQLException("The Plan Generator does not support the function(*) insert the name of the column ");
					}
					
					String function = f.getName().trim();
					TableNode node = findTableByColumn(column);
					if(node == null){
						throw new SQLException("Invalid aggregation column: " + column);
					}
					aggregation.getAttributesOperatorsValues()
							.add(new Condition(removeStringAlias(column), function.toUpperCase(), null));
				}
				
				if(item.getExpression() instanceof Column) {
					String column = item.getExpression().toString().trim();
					
					TableNode node = findTableByColumn(column);
					if(node == null){
						throw new SQLException("Invalid aggregation column: " + column);
					}
					if(apperGroupby(select, column)) {
						aggregation.getAttributesOperatorsValues()
						.add(new Condition(removeStringAlias(column), AggregationOperation.GROUPING, null));						
					}else {
						throw new SQLException("Columar "+column+"are not present in a group by clause");
					}
					
				}
				
				
			
			}

			List<String> orderColumns = new LinkedList<>();
			
			List<Expression> list = select.getGroupByColumnReferences();
			
			for (int i = 0; i < list.size(); i++) {
				String item = list.get(i).toString().trim();
				orderColumns.add(removeStringAlias(item));
				
				for (Condition aov : aggregation.getAttributesOperatorsValues()) {
					if(!aov.getAtribute().equals(removeStringAlias(item))) {
						if(!aov.getOperator().equals(AggregationOperation.GROUPING)) {
							boolean contain = false;
							for (Condition test : aggregation.getAttributesOperatorsValues()) {
								if(test.getAtribute().equals(removeStringAlias(item)) 
										&& test.getOperator().equals(AggregationOperation.GROUPING)) {
									contain = true;
								}
							}
							
							if(!contain)
							aggregation.getAttributesOperatorsValues()
							.add(new Condition(removeStringAlias(item), AggregationOperation.GROUPING, null));
							break;							
						}
					}
				}
				
			}
			
			SortOperation sortOperation = new SortOperation();
			sortOperation.setColumnSorted(orderColumns);
			sortOperation.setPlan(plan);
			
			plan.addOperation(plan.getRoot(),sortOperation);	
			plan.addOperation(plan.getRoot(),aggregation);	
		}
		
		
		
	}
	
	
	private boolean apperGroupby(PlainSelect select,String column) {
		List<Expression> list = select.getGroupByColumnReferences();
		for (int i = 0; i < list.size(); i++) {
			String item = list.get(i).toString().trim();
			if(item.equals(column.trim()))return true;
		}
		return false;
	}
	
	private void addJoinInPlan(JoinEdge join) {
		JoinOperation op = new JoinOperation();
		op.setJoinAlgorithm(new BlockNestedLoopJoin());
		op.setAttributesOperatorsValues(join.aov);
		plan.addOperation(plan.getRoot(), op);
		plan.addOperationDown(false, op,join.right.selectOP != null ? join.right.selectOP : join.right.tableOP);
	}

	private JoinEdge findJoinNotUsed() {
		for (JoinEdge join : joinEdges) {
			if(!join.used){
				return join;
			}
			
		}
		return null;
	}
	
	private JoinEdge findJoin() {
		for (JoinEdge join : joinEdges) {
			if(join.subSelelect){
				return join;
			}
			
		}
		return joinEdges.get(0);
	}
	
	
	
	private void ordeByParse(PlainSelect select) throws SQLException {

		if(select.getOrderByElements() == null){
			return;
		}
		boolean order = true;
		List<String> columns = new LinkedList<>();
		
		for (OrderByElement e : select.getOrderByElements()) {
			String column = e.getExpression().toString().trim();
			order = e.isAsc();
			
			TableNode node = findTableByColumn(column);
			if(node == null){
				throw new SQLException("order by column: " + column + " invalid");
			}
			if(!columns.contains(removeStringAlias(column))){
				columns.add(removeStringAlias(column));				
			}
		}
		
	
		if(!columns.isEmpty()){
			SortOperation sortOperation = new SortOperation();
			sortOperation.setColumnSorted(columns);		
			sortOperation.setOrder(order);
			plan.addOperation(plan.getRoot().getLeft(),sortOperation);
			
		}
		
	}

	private List<Condition> getConditions(Expression e){
		List<Condition> conditions = new LinkedList<>();
		e.accept(new ParseVisitor() {		
			public void visit(AndExpression e) {
				e.getLeftExpression().accept(this);
				e.getRightExpression().accept(this);
			}
			public void visit(OrExpression e) {
				
			}

			public void comparator(ComparisonOperator  e, String operator) {
				conditions.add(new Condition(
						e.getLeftExpression().toString().trim(),
						operator.trim(),
						e.getRightExpression().toString().trim()));
			}
		});

		return conditions;
	}
	

	private void whereParse(Select select) throws SQLException {
		
		
		SelectBody e = select.getSelectBody();
		
		PlainSelect plainSelect = null;
		
		
		if(e instanceof PlainSelect) {
			plainSelect = (PlainSelect) e;
		}
		
		if(plainSelect.getWhere() == null)return;
	

        for (Condition c : getConditions(plainSelect.getWhere())) {
			
			TableNode right = findTableByColumn(c.getRight());
			TableNode left = findTableByColumn(c.getLeft());
			
			if(right!=null && left!=null){
				//LogError.save(this.getClass(),"JOIN!");
				//LogError.save(this.getClass(),right.DB_Table.getName());
				//LogError.save(this.getClass(),left.DB_Table.getName());
				boolean newJoinEdge = true;
				for (JoinEdge join : joinEdges) {
					if( (join.left == left && join.right == right)){
						join.aov.add(new Condition(
								removeStringAlias(c.getLeft()),
								c.getOperator(), 
								removeStringAlias(c.getRight())));
						newJoinEdge = false;
					}else if((join.right == left && join.left == right)){
						join.aov.add(new Condition(
								removeStringAlias(c.getRight()),
								reverseOperator(c.getOperator()), 
								removeStringAlias(c.getLeft())));
						newJoinEdge = false;
					}
				}
				
				
				if(newJoinEdge){
					JoinEdge j = new JoinEdge();
					j.left = left;
					j.right = right;
					j.aov.add(new Condition(
							removeStringAlias(c.getLeft()),
							c.getOperator(), 
							removeStringAlias(c.getRight())));					
				}
				
			}else if(left!=null){
				//LogError.save(this.getClass(),"SELECTION LEFT");
				left.addSelection(new Condition(removeStringAlias(
								c.getLeft()),
								c.getOperator(), 
								c.getRight()));
				
				//LogError.save(this.getClass(),">>>>>>>>>>>>>> "+new AttributeOperatorValueStructure(removeStringAlias(
							//	tExpression.getLeftOperand().toString()),
							//	operator, 
							//	tExpression.getRightOperand().toString()));					
				
				
			}else if(right!=null){
			//	LogError.save(this.getClass(),"SELECTION RIGHT");
				right.addSelection(new Condition(removeStringAlias(
								c.getRight()),
								reverseOperator(c.getOperator()), 
								c.getLeft()));
				
			//	LogError.save(this.getClass(),">>>>>>>>>>>>>> "+new AttributeOperatorValueStructure(removeStringAlias(
						//		tExpression.getRightOperand().toString()),
							//	reverseOperator(operator), 
							//	tExpression.getLeftOperand().toString()));					
				
			}else{
				throw new SQLException("where condition: " + c + " invalid");
			}
			
			//LogError.save(this.getClass(),);
		}
	}
	
	
	private boolean errorTablesFinder = false;
	
	private void tablesFinder(Select select, ISchema schema) throws SQLException {
		errorTablesFinder = false;
		PlainSelect plainSelect = null;
	
		if(select.getSelectBody() instanceof PlainSelect) {
			plainSelect = (PlainSelect) select.getSelectBody();
		}
		
		plainSelect.getFromItem().accept(new FromItemVisitor() {		
			public void visit(ParenthesisFromItem arg0) {}
			public void visit(TableFunction arg0) {}
			public void visit(ValuesList arg0) {}
			public void visit(LateralSubSelect arg0) {}
			public void visit(SubJoin arg0) {}
			public void visit(SubSelect arg0) {}
			
			public void visit(Table arg0) {
				String name = arg0.getName().trim();
				String alias = arg0.getAlias() == null ? null : arg0.getAlias().toString().trim();			
				ITable DB_Table = schema.getTableByName(name);
				if(DB_Table != null) {
					new TableNode(alias,DB_Table);
				}else {
					errorTablesFinder = true;						
				}
			}
		});
		
		if(plainSelect.getJoins()!=null)
		for (Join join: plainSelect.getJoins()) {
		
			join.getRightItem().accept(new FromItemVisitor() {		
				public void visit(ParenthesisFromItem arg0) {}
				public void visit(TableFunction arg0) {}
				public void visit(ValuesList arg0) {}
				public void visit(LateralSubSelect arg0) {}
				public void visit(SubJoin arg0) {}
				public void visit(SubSelect arg0) {}
				
				public void visit(Table arg0) {
					String name = arg0.getName().trim();
					String alias = arg0.getAlias() == null ? null : arg0.getAlias().toString().trim();			
					ITable DB_Table = schema.getTableByName(name);
					if(DB_Table != null) {
						new TableNode(alias,DB_Table);
					}else {
						errorTablesFinder = true;						
					}
				}
			});
			
			if(errorTablesFinder)throw new SQLException("Table: " + join.toString() + " not found in " + schema.getName() + " schema");
			
		}
	}

	
	private void simpleProjectionParse() throws SQLException {
		ProjectionOperation projection = new ProjectionOperation();
		plan.addOperation(plan.getRoot(),projection);
	}
	
	private void projectionParse(Select select) throws SQLException {
		
		PlainSelect plainSelect = null;
		
		if(select.getSelectBody() instanceof PlainSelect) {
			plainSelect = (PlainSelect) select.getSelectBody();
		}

		ProjectionOperation projection = new ProjectionOperation();
		
		String[] attributesProjected = new String[plainSelect.getSelectItems().size()];
		
		if(plainSelect.getSelectItems().get(0).toString().trim().equals("*")) {
			attributesProjected = null;
		}else {
			
			for (int i = 0; i < plainSelect.getSelectItems().size(); i++) {
				
				SelectExpressionItem item = (SelectExpressionItem) plainSelect.getSelectItems().get(i);
				
				if(item.getExpression() instanceof Column) {
					Column c = (Column) item.getExpression();
					attributesProjected[i] = c.getColumnName().trim();
				}
			}
		}
        projection.setAttributesProjected(attributesProjected);
        plan.addOperation(plan.getRoot(),projection);
        	
        
	}
	
	public boolean haveProjectionFunctions(Select select) {
		SelectBody e = select.getSelectBody();

		if(e instanceof PlainSelect) {
			//System.out.println(((PlainSelect) e).getSelectItems());
			PlainSelect plainSelect = (PlainSelect) e;
			if(plainSelect.getSelectItems()!=null)
			for (int i = 0; i < plainSelect.getSelectItems().size(); i++) {
				
				if(plainSelect.getSelectItems().get(i) instanceof SelectExpressionItem) {
					SelectExpressionItem item = (SelectExpressionItem) plainSelect.getSelectItems().get(i);
					
					if(item.getExpression()!=null)
					if(item.getExpression() instanceof Function) {
						return true;
					}	
				}else{
					return false;
				}
			
			}
			
		}
		return false;
}
	
	
	
	/*
	public static void main(String[] args) throws SQLException, InterruptedException, RemoteException {
		
		
		Kernel.start();
		Thread.sleep(500);
		Parse t = new Parse();
		//Plan p = t.parse("SELECT *\n" +
       //         "  FROM partsupp AS ps, part  AS p, supplier s" +
      //          "  WHERE ps.partkey = p.partkey AND s.suppkey = ps.suppkey"
     //           , Kernel.getCatalog().getSchema("tpch")) ;
		Plan p = t.parse(
				"SELECT s_acctbal, s_name, n_name, p.partkey, p_mfgr, s_address, s_phone, s_comment "+
				"FROM part p, supplier s, partsupp ps, nation n, region r "+
				"WHERE p.partkey = ps.partkey and "+
				"s.suppkey = ps.suppkey and "+
				"s.nationkey = n.nationkey and "+
				"n.regionkey = r.regionkey and "+
				"p.p_size = 20 and "+
				"r.r_name = 'EUROPE' "
                , Kernel.getCatalog().getSchemabyName("tpch")) ;
		
		
		DBConnection connection = new DBConnection("tpch"); //.getInstance().getLocalConnection("tpch", "admin", "admin");
		Kernel.getExecuteTransactions().execute(new TransactionRunnable() {
			
			public void run(Transaction transaction) {
				p.setTransaction(transaction);
				ITable temp = p.execute();
				LogError.save(this.getClass(),p.getRoot().getName());
				LogError.save(this.getClass(),temp.getNumberOfTuples(transaction));
				
				DrawTable ij = new DrawTable( transaction,temp);
				ij.reloadMatriz();
				
			}
			
		},connection);
		
      
	}
*/	
	private String removeStringAlias(String col){
		String column[] = col.split("\\.");
		if(column.length == 2 && !col.contains("'") && !col.contains("\"")  ){
			return column[1];
		}else{
			return col;
		}
	}
	
	private String reverseOperator(String s){
		if(s.equals("<="))return s = ">=";
		if(s.equals(">="))return s = "<=";
		if(s.equals(">"))return s = "<";
		if(s.equals("<"))return s = ">";
		return s;
	}
	private TableNode findTableByColumn(String col){
		col = col.trim();
		String column[] = col.split("\\.");
		if(column.length == 2 && !col.contains("'") && !col.contains("\"")  ){
			
			for (TableNode tn : tablesNodes) {
				
				if(tn.subPlan!=null)continue;
				
				if(tn.alias.contains(column[0]) || tn.DBTable.getName().equals(column[0])){
					
					return tn.DBTable.getIdColumn(column[1])>= 0 ? tn : null;
				}
			}
		}else{
			for (TableNode tn : tablesNodes) {
				if(tn.subPlan!=null)continue;
				if(tn.DBTable.getIdColumn(col)>= 0){
					return tn;
				}
			}
		}
		return null;
	}
	
	
}
