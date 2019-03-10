package graphicalInterface.util;

import java.awt.Color;

import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultStyledDocument;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;

public class SQLHighlighter extends DefaultStyledDocument {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final StyleContext cont = StyleContext.getDefaultStyleContext();
	private final AttributeSet attrBlue = cont.addAttribute(cont.getEmptySet(), StyleConstants.Foreground, Color.BLUE);
	private final AttributeSet attrBlack = cont.addAttribute(cont.getEmptySet(), StyleConstants.Foreground, Color.BLACK);
	//private final AttributeSet attrRed = cont.addAttribute(cont.getEmptySet(), StyleConstants.Foreground, Color.RED);

	
	private final String RESERVAD_WORDS = "(\\W)*(" +buildReservadWords()+")";
	
	private String buildReservadWords(){
		StringBuffer sb = new StringBuffer();
	
		sb.append("VALUES|");
		sb.append("INTO|");
		sb.append("INSERT|");
		sb.append("SELECT|");
		sb.append("FROM|");
		sb.append("WHERE|");
		sb.append("AS|");
		sb.append("ORDER|");
		sb.append("BY|");
		sb.append("AND|");
		sb.append("OR|");
		sb.append("CREATE|");
		sb.append("TABLE|");
		sb.append("DATABASE|");
		sb.append("UPDATE|");
		sb.append("DELETE|");
		sb.append("SET|");
		sb.append("BACKUP|");
		sb.append("COMMIT|");
		sb.append("ABORT|");
		sb.append("ROLLBACK|");
		sb.append("ASC|");
		sb.append("DESC|");
		sb.append("DROP|");
		sb.append("IN|");
		sb.append("NOT|");
		sb.append("DISTINCT|");
		sb.append("GROUP|");
		sb.append("EXISTS");
		
	
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
