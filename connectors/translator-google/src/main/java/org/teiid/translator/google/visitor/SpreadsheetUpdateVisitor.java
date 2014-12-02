package org.teiid.translator.google.visitor;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.resource.adapter.google.common.UpdateSet;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;

/**
 * Translates SQL UPDATE commands
 * 
 * 
 * @author felias
 * 
 */
public class SpreadsheetUpdateVisitor extends SpreadsheetCriteriaVisitor {

	public SpreadsheetUpdateVisitor(SpreadsheetInfo info) {
		super(info);
	}

	private List<UpdateSet> changes;

	public void visit(Update obj) {
		worksheetTitle = obj.getTable().getName();
		changes = new ArrayList<UpdateSet>();
		if (obj.getTable().getMetadataObject().getNameInSource() != null) {
			this.worksheetTitle = obj.getTable().getMetadataObject().getNameInSource();
		}
		worksheetKey = info.getWorksheetByName(worksheetTitle).getId();
		for (SetClause s : obj.getChanges()) {
			SQLStringVisitor.getSQLString(s.getValue());
			if (s.getSymbol().getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
				changes.add(new UpdateSet(s.getSymbol().getName(),"'"+getStringValue(s.getValue())));
			} else {
				changes.add(new UpdateSet(s.getSymbol().getName(), getStringValue(s.getValue())));
			}
		}
		if (obj.getWhere() != null) {
			append(obj.getWhere());
			criteriaQuery = buffer.toString();
		} else {
			criteriaQuery = "";
		}

	}

	public List<UpdateSet> getChanges() {
		return changes;
	}

	public void setChanges(List<UpdateSet> changes) {
		this.changes = changes;
	}

}
