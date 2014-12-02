package org.teiid.translator.google.visitor;

import static org.teiid.language.SQLConstants.Reserved.NULL;

import java.text.SimpleDateFormat;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Expression;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;

/**
 * Base visitor for criteria in the UPDATE and DELETE commands
 * 
 * @author felias
 * 
 */
public class SpreadsheetCriteriaVisitor extends SQLStringVisitor {

	protected String worksheetKey;
	protected String criteriaQuery;
	protected SpreadsheetInfo info;
	protected boolean headerEnabled;
	protected String worksheetTitle;

	public SpreadsheetCriteriaVisitor(SpreadsheetInfo info) {
		this.info = info;
	}

	public void visit(Literal obj) {
		if (obj.getValue() == null) {
			buffer.append(NULL);
			return;
		}
		Class<?> type = obj.getType();
		if (Number.class.isAssignableFrom(type)) {
			buffer.append(obj.toString());
			return;
		} else if (obj.getType().equals(DataTypeManager.DefaultDataClasses.DATE)) {
			buffer.append(new java.text.SimpleDateFormat("yyyy-MM-dd").format(obj.getValue()));
			return;
		} else {
			buffer.append("\"");
			buffer.append(obj.getValue().toString());
			buffer.append("\"");
			return;
		}
	}

	protected String getStringValue(Expression obj) {
		Literal literal;
		if (obj instanceof Literal) {
			literal = (Literal) obj;
		} else {
			throw new SpreadsheetOperationException("Spreadsheet translator internal error: Expression is not allowed in the set clause");
		}
		if (literal.getType().equals(DataTypeManager.DefaultDataClasses.DATE)) {
			return new java.text.SimpleDateFormat("MM/dd/yyyy").format(literal.getValue());
		} else if (literal.getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
			return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(literal.getValue());
		} else if (literal.getType().equals(DataTypeManager.DefaultDataClasses.TIME)) {
			return new SimpleDateFormat("HH:mm:ss").format(literal.getValue());
		} else
			return literal.getValue().toString();
	}

	public void visit(Like obj) {
		throw new SpreadsheetOperationException("Like is not supported in DELETE and UPDATE queires");
	}

	@Override
	protected String replaceElementName(String group, String element) {
		return element.toLowerCase();
	}

	public String getWorksheetKey() {
		return worksheetKey;
	}

	public String getCriteriaQuery() {
		return criteriaQuery;
	}

	public void setCriteriaQuery(String criteriaQuery) {
		this.criteriaQuery = criteriaQuery;
	}

	public boolean isHeaderEnabled() {
		return headerEnabled;
	}

	public void setHeaderEnabled(boolean headerEnabled) {
		this.headerEnabled = headerEnabled;
	}

	public String getWorksheetTitle() {
		return worksheetTitle;
	}

}
