package graphicalInterface.util;

import java.awt.Color;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;

import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultStyledDocument;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;

public class RelationalCalculusHighlighter extends DefaultStyledDocument {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final StyleContext cont = StyleContext.getDefaultStyleContext();
	private final AttributeSet attrBlue = cont.addAttribute(cont.getEmptySet(), StyleConstants.Foreground, Color.BLUE);
	private final AttributeSet attrBlack = cont.addAttribute(cont.getEmptySet(), StyleConstants.Foreground, Color.BLACK);
	//private final AttributeSet attrRed = cont.addAttribute(cont.getEmptySet(), StyleConstants.Foreground, Color.RED);
	
	private static Properties props = null;
	
	private static String getProp(String key){
		if(props == null) {
			props = new Properties();
			
			try {
				props.load(new InputStreamReader(RelationalCalculusHighlighter.class.getResourceAsStream("RC-operations.properties"),Charset.forName("UTF-8")));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return props.getProperty(key);
	}

	
	public static String FORALL = getProp("FORALL");
	public static String EXISTS = getProp("EXISTS");
	public static String AND = getProp("AND");
	public static String OR = getProp("OR");
	public static String IMPLICATES = getProp("IMPLICATES");
	public static String NOT = getProp("NOT");
	public static String DIFFERENT = getProp("DIFFERENT");
	public static String GREATER_OR_EQUAL = getProp("GREATER_OR_EQUAL");
	public static String LESS_OR_EQUAl = getProp("LESS_OR_EQUAl");
	public static String GREATER = getProp("GREATER");
	public static String LESS = getProp("LESS");
	public static String EQUAL = getProp("EQUAL");
	
	private final String RESERVAD_WORDS = "(\\W)*(" +buildReservadWords()+")";
	
	
	
	
	private String buildReservadWords(){
		StringBuffer sb = new StringBuffer();
	
		sb.append(FORALL+"|");
		sb.append(EXISTS+"|");
		sb.append(AND+"|");
		sb.append(OR+"|");
		sb.append(IMPLICATES+"|");
		sb.append(NOT+"|");
		sb.append(DIFFERENT+"|");
		sb.append(GREATER+"|");
		sb.append(LESS+"|");
		sb.append(GREATER_OR_EQUAL+"|");
		sb.append(LESS_OR_EQUAl+"|");
		sb.append(EQUAL+"");
	;
	
		///sb.append("\".*\"");
		return sb.toString();
	}
	
	
	
	public void insertString(int offset, String str, AttributeSet a) throws BadLocationException {
		super.insertString(offset, str, a);

		String text = getText(0, getLength());
		int before = findLastNonWordChar(text, offset);
		if (before < 0)
			before = 0;
		int after = findFirstNonWordChar(text, offset + str.length());
		int wordL = before;
		int wordR = before;

		while (wordR <= after) {
			
			if (wordR == after || String.valueOf(text.charAt(wordR)).matches("\\W")) {
				
				if (text.substring(wordL, wordR).toUpperCase().matches(RESERVAD_WORDS)){
					setCharacterAttributes(wordL, wordR - wordL, attrBlue, false);
					
				}else{
					
					setCharacterAttributes(wordL, wordR - wordL, attrBlack, false);											
					
				}
				wordL = wordR;
			}
			wordR++;
		}
	}

	public void remove(int offs, int len) throws BadLocationException {
		super.remove(offs, len);

		String text = getText(0, getLength());
		int before = findLastNonWordChar(text, offs);
		if (before < 0)
			before = 0;
		int after = findFirstNonWordChar(text, offs);

		if (text.substring(before, after).matches(RESERVAD_WORDS)) {
			setCharacterAttributes(before, after - before, attrBlue, false);
		} else {
			setCharacterAttributes(before, after - before, attrBlack, false);
		}
	}

	private int findLastNonWordChar(String text, int index) {
		while (--index >= 0) {
			if (String.valueOf(text.charAt(index)).matches("\\W")) {
				break;
			}
		}
		return index;
	}

	private int findFirstNonWordChar(String text, int index) {
		while (index < text.length()) {
			if (String.valueOf(text.charAt(index)).matches("\\W")) {
				break;
			}
			index++;
		}
		return index;
	}

}
