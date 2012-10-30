package org.teiid.translator.google.execution;

import static org.teiid.language.SQLConstants.Reserved.SELECT;
import static org.teiid.language.SQLConstants.Reserved.WHERE;

import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;

public class SpreadsheetSQLVisitor extends SQLStringVisitor {

	private String worksheetTitle;
	private Integer limitValue = null;
	private Integer offsetValue = null;


	public String getWorksheetTitle() {
		return worksheetTitle;
	}

	/**
	 * Return only col name e.g. "A"
	 */
	@Override
	protected String replaceElementName(String group, String element) {
		return element;
	}

	public String getTranslatedSQL() {
		return buffer.toString();
	}

	public void translateSQL(LanguageObject obj) {
		append(obj);
	}

	public void visit(Select obj) {

		buffer.append(SELECT).append(Tokens.SPACE);
		if (useSelectLimit() && obj.getLimit() != null) {
			append(obj.getLimit());
			buffer.append(Tokens.SPACE);
		}
		append(obj.getDerivedColumns());
		if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
			append(obj.getFrom());
		}
		if (obj.getWhere() != null) {
			buffer.append(Tokens.SPACE).append(WHERE).append(Tokens.SPACE);
			append(obj.getWhere());
		}
		if (obj.getGroupBy() != null) {
			buffer.append(Tokens.SPACE);
			append(obj.getGroupBy());
		}
		if (obj.getOrderBy() != null) {
			buffer.append(Tokens.SPACE);
			append(obj.getOrderBy());
		}
		if (!useSelectLimit() && obj.getLimit() != null) {
			append(obj.getLimit());
		}
	}

	public void visit(NamedTable obj) {		
		this.worksheetTitle = obj.getName();	
	}

	public void visit(Limit obj) {
		if (obj.getRowOffset() > 0) {
			offsetValue = obj.getRowOffset();
		}
		limitValue = obj.getRowLimit();
	}

	public Integer getLimitValue() {
		return limitValue;
	}

	public Integer getOffsetValue() {
		return offsetValue;
	}


}
