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

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
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
		String columnName;
		if (obj.getTable().getMetadataObject().getNameInSource() != null) {
			this.worksheetTitle = obj.getTable().getMetadataObject().getNameInSource();
		}
		worksheetKey = info.getWorksheetByName(worksheetTitle).getId();
		for (SetClause s : obj.getChanges()) {
			if(s.getSymbol().getMetadataObject().getNameInSource()!=null){
				columnName=s.getSymbol().getMetadataObject().getNameInSource();
			}else{
				columnName=s.getSymbol().getMetadataObject().getName();
			}
			if (s.getSymbol().getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
				changes.add(new UpdateSet(columnName,"'"+getStringValue(s.getValue()))); //$NON-NLS-1$
			} else {
				changes.add(new UpdateSet(columnName, getStringValue(s.getValue())));
			}
		}
		if (obj.getWhere() != null) {
			append(obj.getWhere());
			criteriaQuery = buffer.toString();
		} else {
			criteriaQuery = ""; //$NON-NLS-1$
		}

	}

	public List<UpdateSet> getChanges() {
		return changes;
	}

	public void setChanges(List<UpdateSet> changes) {
		this.changes = changes;
	}

}
