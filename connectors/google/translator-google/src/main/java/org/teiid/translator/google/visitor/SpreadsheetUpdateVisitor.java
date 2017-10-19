/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator.google.visitor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Expression;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.translator.goole.api.SpreadsheetOperationException;
import org.teiid.translator.goole.api.UpdateSet;
import org.teiid.translator.goole.api.metadata.SpreadsheetInfo;

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
		String columnName;
		if (obj.getTable().getMetadataObject().getNameInSource() != null) {
			this.worksheetTitle = obj.getTable().getMetadataObject().getNameInSource();
		}
		for (SetClause s : obj.getChanges()) {
			if(s.getSymbol().getMetadataObject().getNameInSource()!=null){
				columnName=s.getSymbol().getMetadataObject().getNameInSource();
			}else{
				columnName=s.getSymbol().getMetadataObject().getName();
			}
			changes.add(new UpdateSet(columnName, getStringValue(s.getValue())));
		}
		translateWhere(obj.getWhere());
	}
	
	protected String getStringValue(Expression obj) {
        Literal literal;
        if (obj instanceof Literal) {
            literal = (Literal) obj;
        } else {
            throw new SpreadsheetOperationException("Spreadsheet translator internal error: Expression is not allowed in the set clause"); //$NON-NLS-1$
        }
        if (literal.getValue() == null) {
            if (literal.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
                throw new SpreadsheetOperationException("Spreadsheet translator error: String values cannot be set to null"); //$NON-NLS-1$
            }
            return ""; //$NON-NLS-1$
        }
        if (literal.getType().equals(DataTypeManager.DefaultDataClasses.DATE)) {
            return new java.text.SimpleDateFormat("MM/dd/yyyy").format(literal.getValue());
        } else if (literal.getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            return new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(literal.getValue());
        } 
        if (literal.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
            return "'"+literal.getValue().toString();
        }
        return literal.getValue().toString();
    }

	public List<UpdateSet> getChanges() {
		return changes;
	}

	public void setChanges(List<UpdateSet> changes) {
		this.changes = changes;
	}

}
