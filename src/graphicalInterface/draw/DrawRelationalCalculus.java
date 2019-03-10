package graphicalInterface.draw;


import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextPane;
import javax.swing.text.BadLocationException;

import DBMS.Kernel;
import DBMS.connectionManager.DBConnection;
import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.relationalCalculusToSql.SealDBCatalogAdapter;
import DBMS.queryProcessing.relationalCalculusToSql.trcGrammar.ParseException;
import DBMS.queryProcessing.relationalCalculusToSql.trcGrammar.TrcGrammar;
import DBMS.queryProcessing.relationalCalculusToSql.trcQueryElements.Query;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.VisitorSQLNF;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.VisitorToSQL;
import DBMS.queryProcessing.relationalCalculusToSql.visitors.VisitorToString;
import graphicalInterface.util.RelationalCalculusHighlighter;

public class DrawRelationalCalculus extends JPanel{

	private static final long serialVersionUID = 1L;


	JTextPane projectionTextArea;
	JTextPane predicateTextArea;
	JTextPane output;
	JTextPane outputNF;

	public DrawRelationalCalculus(DBConnection connection, JTextPane textSQL, JTabbedPane tabs) {
		super();
		this.setBackground(Color.WHITE);

		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel.rowHeights = new int[]{25, 0, 0, 0, 0, 0};
		gbl_panel.columnWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gbl_panel);
		
		
		JPanel labels = new JPanel();
		labels.setBackground(Color.WHITE);
		GridBagConstraints gbc_scrollPane1 = new GridBagConstraints();
		gbc_scrollPane1.gridheight = 4;
		gbc_scrollPane1.gridwidth = 10;
		gbc_scrollPane1.insets = new Insets(0, 0, 5, 5);
		gbc_scrollPane1.fill = GridBagConstraints.BOTH;
		gbc_scrollPane1.gridx = 0;
		gbc_scrollPane1.gridy = 0;
		this.add(labels, gbc_scrollPane1);
		GridBagLayout gbl_labels = new GridBagLayout();
		gbl_labels.columnWidths = new int[]{0, 0, 0, 0, 0};
		gbl_labels.rowHeights = new int[]{0, 25, 25, 0, 25, 0, 0};
		gbl_labels.columnWeights = new double[]{0.0, 0.0, 1.0, 0.0, Double.MIN_VALUE};
		gbl_labels.rowWeights = new double[]{0.0, 1.0, 0.0, 1.0, 0.0, 0.0, Double.MIN_VALUE};
		labels.setLayout(gbl_labels);
		
		JLabel space0 = new JLabel(" ");
		GridBagConstraints gbc_space0 = new GridBagConstraints();
		gbc_space0.anchor = GridBagConstraints.NORTHWEST;
		gbc_space0.insets = new Insets(0, 0, 5, 5);
		gbc_space0.gridx = 1;
		gbc_space0.gridy = 0;
		labels.add(space0, gbc_space0);
		
		JLabel space1 = new JLabel(" ");
		GridBagConstraints gbc_space1 = new GridBagConstraints();
		gbc_space1.insets = new Insets(0, 0, 5, 5);
		gbc_space1.gridx = 0;
		gbc_space1.gridy = 1;
		labels.add(space1, gbc_space1);
		
		JLabel lblProjection = new JLabel("Projection");
		GridBagConstraints gbc_lblProjection = new GridBagConstraints();
		gbc_lblProjection.fill = GridBagConstraints.HORIZONTAL;
		gbc_lblProjection.insets = new Insets(0, 0, 5, 5);
		gbc_lblProjection.gridx = 1;
		gbc_lblProjection.gridy = 1;
		labels.add(lblProjection, gbc_lblProjection);
		
		JScrollPane scrollPane1 = new JScrollPane();
		GridBagConstraints gbc_scrollPane11 = new GridBagConstraints();
		gbc_scrollPane11.fill = GridBagConstraints.BOTH;
		gbc_scrollPane11.insets = new Insets(0, 0, 5, 5);
		gbc_scrollPane11.gridx = 2;
		gbc_scrollPane11.gridy = 1;
		labels.add(scrollPane1, gbc_scrollPane11);
		
		projectionTextArea = new JTextPane(new RelationalCalculusHighlighter());
		projectionTextArea.setText("p.p_name, p.p_type, s.s_name");
		scrollPane1.setViewportView(projectionTextArea);
		
		JLabel space2 = new JLabel(" ");
		GridBagConstraints gbc_space2 = new GridBagConstraints();
		gbc_space2.insets = new Insets(0, 0, 5, 0);
		gbc_space2.gridx = 3;
		gbc_space2.gridy = 1;
		labels.add(space2, gbc_space2);
		
		JLabel lblPredicate = new JLabel("Predicate");
		GridBagConstraints gbc_lblPredicate = new GridBagConstraints();
		gbc_lblPredicate.fill = GridBagConstraints.HORIZONTAL;
		gbc_lblPredicate.insets = new Insets(0, 0, 5, 5);
		gbc_lblPredicate.gridx = 1;
		gbc_lblPredicate.gridy = 2;
		labels.add(lblPredicate, gbc_lblPredicate);
		
		JPanel predicatePanel = new JPanel();
		GridBagConstraints gbc_predicatePanel = new GridBagConstraints();
		gbc_predicatePanel.insets = new Insets(0, 0, 5, 5);
		gbc_predicatePanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_predicatePanel.gridx = 2;
		gbc_predicatePanel.gridy = 2;
		labels.add(predicatePanel, gbc_predicatePanel);
		predicatePanel.setLayout(new GridLayout(2, 6));
	
		LinkedList<JButton> buttons = new LinkedList<>(); 
		
		
		buttons.add(new JButton(RelationalCalculusHighlighter.FORALL));
		buttons.add(new JButton(RelationalCalculusHighlighter.EXISTS));
		buttons.add(new JButton(RelationalCalculusHighlighter.AND));
		buttons.add(new JButton(RelationalCalculusHighlighter.OR));
		buttons.add(new JButton(RelationalCalculusHighlighter.IMPLICATES));
		buttons.add(new JButton(RelationalCalculusHighlighter.NOT));
		buttons.add(new JButton(RelationalCalculusHighlighter.EQUAL));
		buttons.add(new JButton(RelationalCalculusHighlighter.DIFFERENT));
		buttons.add(new JButton(RelationalCalculusHighlighter.GREATER_OR_EQUAL));
		buttons.add(new JButton(RelationalCalculusHighlighter.LESS_OR_EQUAl));
		buttons.add(new JButton(RelationalCalculusHighlighter.GREATER));
		buttons.add(new JButton(RelationalCalculusHighlighter.LESS));
		
		
		
		for (JButton jb : buttons) {
			predicatePanel.add(jb);
			jb.addActionListener(new ActionListener() {
		
				public void actionPerformed(ActionEvent e) {
					
						//predicateTextArea.setText(predicateTextArea.getText()+ " "+ jb.getText());
						try {
							predicateTextArea.getDocument().insertString(predicateTextArea.getCaretPosition(), jb.getText(), null);
						} catch (BadLocationException e1) {
							// TODO Auto-generated catch block
							predicateTextArea.setText(predicateTextArea.getText()+ " "+ jb.getText());
						}
				}
			});
		}
		
		JScrollPane scrollPane2 = new JScrollPane();
		GridBagConstraints gbc_scrollPane2 = new GridBagConstraints();
		gbc_scrollPane2.gridwidth = 2;
		gbc_scrollPane2.fill = GridBagConstraints.BOTH;
		gbc_scrollPane2.insets = new Insets(0, 0, 5, 5);
		gbc_scrollPane2.gridx = 1;
		gbc_scrollPane2.gridy = 3;
		labels.add(scrollPane2, gbc_scrollPane2);
		
		predicateTextArea = new JTextPane(new RelationalCalculusHighlighter());
		predicateTextArea.setText("part(p) ∧ partsupp(ps) ∧ supplier(s) ∧ (p.partkey = ps.partkey)  ∧ (s.suppkey = ps.suppkey)");
		scrollPane2.setViewportView(predicateTextArea);
		
		JLabel outFormula = new JLabel("OUTPUT Formula:");
		GridBagConstraints gbc_outFormula = new GridBagConstraints();
		gbc_outFormula.anchor = GridBagConstraints.WEST;
		gbc_outFormula.insets = new Insets(0, 0, 5, 5);
		gbc_outFormula.gridx = 1;
		gbc_outFormula.gridy = 4;
		labels.add(outFormula, gbc_outFormula);
		
		JScrollPane scrollPane3 = new JScrollPane();
		GridBagConstraints gbc_scrollPane3 = new GridBagConstraints();
		gbc_scrollPane3.fill = GridBagConstraints.BOTH;
		gbc_scrollPane3.insets = new Insets(0, 0, 5, 5);
		gbc_scrollPane3.gridx = 2;
		gbc_scrollPane3.gridy = 4;
		labels.add(scrollPane3, gbc_scrollPane3);
		
		output = new JTextPane(new RelationalCalculusHighlighter());
		scrollPane3.setViewportView(output);
		output.setEditable(false);
		
		JLabel lblSqlnfFormula = new JLabel("SQLNF Formula:");
		GridBagConstraints gbc_lblSqlnfFormula = new GridBagConstraints();
		gbc_lblSqlnfFormula.anchor = GridBagConstraints.WEST;
		gbc_lblSqlnfFormula.insets = new Insets(0, 0, 0, 5);
		gbc_lblSqlnfFormula.gridx = 1;
		gbc_lblSqlnfFormula.gridy = 5;
		labels.add(lblSqlnfFormula, gbc_lblSqlnfFormula);
		
		JScrollPane scrollPane4 = new JScrollPane();
		GridBagConstraints gbc_scrollPane4 = new GridBagConstraints();
		gbc_scrollPane4.insets = new Insets(0, 0, 0, 5);
		gbc_scrollPane4.fill = GridBagConstraints.BOTH;
		gbc_scrollPane4.gridx = 2;
		gbc_scrollPane4.gridy = 5;
		labels.add(scrollPane4, gbc_scrollPane4);
		
		outputNF = new JTextPane(new RelationalCalculusHighlighter());
		scrollPane4.setViewportView(outputNF);
		outputNF.setEditable(false);
		
		JButton buildFormula = new JButton("Build Formula");
		GridBagConstraints gbc_buildFormula = new GridBagConstraints();
		gbc_buildFormula.gridx = 7;
		gbc_buildFormula.gridy = 4;
		this.add(buildFormula, gbc_buildFormula);
		
		buildFormula.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {

				buildFormula(Kernel.getCatalog().getSchemabyName(connection.getSchemaName()));
			}
		});
		
		JButton toSQL = new JButton("Transform to SQL");
		toSQL.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				
				String sql = buildFormula(Kernel.getCatalog().getSchemabyName(connection.getSchemaName()));
				if(sql != null) {
					textSQL.setText(sql);
					tabs.setSelectedIndex(0);
				}
				
			}
		});
		GridBagConstraints gbc_toSQL = new GridBagConstraints();
		gbc_toSQL.gridx = 9;
		gbc_toSQL.gridy = 4;
		this.add(toSQL, gbc_toSQL);
	}
	
	public String buildFormula(ISchema schema) {
		
		if(projectionTextArea.getText().isEmpty() || predicateTextArea.getText().isEmpty() || schema == null) return null;
		
		
		
		String stringQuery = "{"+ 
				projectionTextArea.getText() + " | " +
				predicateTextArea.getText()
				.replaceAll(RelationalCalculusHighlighter.FORALL,"FORALL")
				.replace(RelationalCalculusHighlighter.EXISTS, "EXISTS")
				.replace(RelationalCalculusHighlighter.AND, "AND")
				.replace(RelationalCalculusHighlighter.OR, "OR")
				.replace(RelationalCalculusHighlighter.IMPLICATES, "->")
				.replace(RelationalCalculusHighlighter.NOT, "NOT")
				.replace(RelationalCalculusHighlighter.DIFFERENT, "!=")
				.replace(RelationalCalculusHighlighter.GREATER_OR_EQUAL, ">=")
				.replace(RelationalCalculusHighlighter.LESS_OR_EQUAl, "<=") 
				
				+"}"; 
		
		TrcGrammar parser = new TrcGrammar(new ByteArrayInputStream(stringQuery.getBytes()));

		output.setText(stringQuery);
		
		Query p = null;
		try {
			p = parser.query();
		} catch (ParseException e) {
			JOptionPane.showMessageDialog(null, e.getMessage());	
			return null;
		} 
		
		HashMap<String, HashSet<String>> dbSchema = SealDBCatalogAdapter.getDbSchema(schema);
		VisitorToString v = new VisitorToString();
		VisitorToSQL vSql = new VisitorToSQL(dbSchema);
		
		
		p.accept(new VisitorSQLNF());
		p.accept(v);
		
		String stringSqlnf = v.stringResult;
		
		String stringSQL = p.accept(vSql);
		
		outputNF.setText(stringSqlnf);
		
		String error = "";
		
		if(vSql.getErrorLog().hasFormulaError()) {
			error+="Formula Error: \n";
			for(String e : vSql.getErrorLog().getFormulaErrors()) {
				error+=e;
			}
		}
		
		if(vSql.getErrorLog().hasScopeError()) {
			error+="Scope Error: \n";
			for(String e : vSql.getErrorLog().getScopeErrors()) {
				error+=e+"\n";
			}
		}
	
		
		if(!error.isEmpty()) {			
			JOptionPane.showMessageDialog(null, error);	
		}else{
		
			return stringSQL;
		}
		
		return null;
	}
	
}
