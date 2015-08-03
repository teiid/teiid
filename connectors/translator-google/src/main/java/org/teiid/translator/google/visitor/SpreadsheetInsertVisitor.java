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

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;

/**
 * Translates SQL INSERT commands
 * 
 * @author felias
 * 
 */
public class SpreadsheetInsertVisitor extends SQLStringVisitor {
	private String worksheetKey;
	private List<String> columns;
	private List<String> values;
	private Map<String, String> columnNameValuePair;
	SpreadsheetInfo info;
	private String worksheetTitle;

	public SpreadsheetInsertVisitor(SpreadsheetInfo info) {
		this.info = info;
		columnNameValuePair = new HashMap<String, String>();
		columns = new ArrayList<String>();
		values = new ArrayList<String>();
	}

	public void visit(Insert obj) {
		worksheetTitle = obj.getTable().getName();
		if (obj.getTable().getMetadataObject().getNameInSource() != null) {
			worksheetTitle = obj.getTable().getMetadataObject().getNameInSource();
		}
		worksheetKey = info.getWorksheetByName(worksheetTitle).getId();
		super.visit(obj);
		for (int i = 0; i < values.size() && i < columns.size(); i++) {
			columnNameValuePair.put(columns.get(i), values.get(i));
		}
	}

	@Override
	public void visit(ColumnReference obj) {
		if(obj.getMetadataObject().getNameInSource()!=null){
			columns.add(obj.getMetadataObject().getNameInSource());
		}else{
			columns.add(obj.getMetadataObject().getName());
		}
		super.visit(obj);
	}

	@Override
	public void visit(ExpressionValueSource obj) {
		for (Expression e : obj.getValues()) {
			if (!(e instanceof Literal)) {
				throw new SpreadsheetOperationException("Only literals are allowed in the values section");
			}
		}
		super.visit(obj);

	}

	public void visit(Literal obj) {
		if (obj.getValue() == null) {
			buffer.append(NULL);
			return;
		}
		Class<?> type = obj.getType();
		if (Number.class.isAssignableFrom(type)) {
			values.add(obj.getValue().toString());
			return;
		} else if (type.equals(DataTypeManager.DefaultDataClasses.STRING)) {
			values.add("'"+obj.getValue().toString());
		} else {
			values.add(obj.getValue().toString());
			return;
		}
	}

	public String getWorksheetKey() {
		return worksheetKey;
	}

	public Map<String, String> getColumnNameValuePair() {
		return columnNameValuePair;
	}

	public String getWorksheetTitle() {
		return worksheetTitle;
	}

}
