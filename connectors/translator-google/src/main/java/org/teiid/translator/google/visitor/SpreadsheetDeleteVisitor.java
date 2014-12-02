package org.teiid.translator.google.visitor;

import org.teiid.language.Delete;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
/**
 * Translates SQL DELETE commands
 * 
 * @author felias
 *
 */
public class SpreadsheetDeleteVisitor extends SpreadsheetCriteriaVisitor {

	
	public SpreadsheetDeleteVisitor(SpreadsheetInfo info) {
		super(info);
	}

	public void visit(Delete obj) {
		worksheetTitle = obj.getTable().getName();	
		worksheetKey = info.getWorksheetByName(worksheetTitle).getId();
		
		if (obj.getWhere() != null) {
			append(obj.getWhere());
			criteriaQuery = buffer.toString();
		} else {
			criteriaQuery = "";
		}
	}

}
