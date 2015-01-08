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

import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.google.SpreadsheetExecutionFactory;
/**
 * Translates SQL SELECT queries
 * 
 * @author felias
 *
 */
public class SpreadsheetSQLVisitor extends SQLStringVisitor {

	private String worksheetTitle;
	private Integer limitValue = null;
	private Integer offsetValue = null;
    private SpreadsheetInfo info;

	public SpreadsheetSQLVisitor(SpreadsheetInfo spreadsheetInfo) {
		this.info=spreadsheetInfo;
	}

	public String getWorksheetTitle() {
		return worksheetTitle;
	}

	/**
	 * Return only col name e.g. "A"
	 */
	@Override
	protected String replaceElementName(String group, String element) {
		Worksheet worksheetSourceName=info.getWorksheetByName(worksheetTitle);
		if(worksheetSourceName==null){
			throw new SpreadsheetOperationException(SpreadsheetExecutionFactory.UTIL.gs("missing_worksheet", worksheetTitle)); //$NON-NLS-1$
		}
		String columnId=worksheetSourceName.getColumnID(element);
 	    if(columnId==null){
 	    	throw new SpreadsheetOperationException("Column "+element +" doesn't exist in the worksheet "+worksheetTitle);
 	    }
		return columnId;
	}

	public String getTranslatedSQL() {
		return buffer.toString();
	}

	public void translateSQL(LanguageObject obj) {
		append(obj);
	}

	public void visit(Select obj) {
		buffer.append(SELECT).append(Tokens.SPACE);
		
		if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
			NamedTable table = ((NamedTable)obj.getFrom().get(0));
			this.worksheetTitle = table.getName();
			if (table.getMetadataObject().getNameInSource() != null) {
				this.worksheetTitle = table.getMetadataObject().getNameInSource();
			}
		}
		append(obj.getDerivedColumns());
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
		if (obj.getLimit() != null) {
			if (obj.getLimit().getRowOffset() > 0) {
				offsetValue = obj.getLimit().getRowOffset();
			}
			limitValue = obj.getLimit().getRowLimit();
		}
	}

	public Integer getLimitValue() {
		return limitValue;
	}

	public Integer getOffsetValue() {
		return offsetValue;
	}
	
	@Override
	public void visit(Function function) {
		if (function.getName().equalsIgnoreCase(SourceSystemFunctions.DAYOFMONTH)) {
			function.setName("day"); //$NON-NLS-1$
		} else if (function.getName().equalsIgnoreCase(SourceSystemFunctions.UCASE)) {
			function.setName("upper"); //$NON-NLS-1$
		} else if (function.getName().equalsIgnoreCase(SourceSystemFunctions.LCASE)) {
			function.setName("lower"); //$NON-NLS-1$
		} else if (function.getName().equalsIgnoreCase(SourceSystemFunctions.DAYOFWEEK)) {
			function.setName("weekday"); //$NON-NLS-1$
		}
		super.visit(function);
	}
	
	@Override
	public void visit(Literal obj) {
		if (obj.getValue() == null) {
			super.visit(obj);
		} else if (obj.getType() == TypeFacility.RUNTIME_TYPES.DATE) {
			buffer.append("date ").append('\"').append(obj.getValue()).append('\"'); //$NON-NLS-1$
		} else if (obj.getType() == TypeFacility.RUNTIME_TYPES.TIME) {
			buffer.append("timeofday ").append('\"').append(obj.getValue()).append('\"'); //$NON-NLS-1$
		} else if (obj.getType() == TypeFacility.RUNTIME_TYPES.TIMESTAMP) {
			String val = obj.getValue().toString();
			int i = val.lastIndexOf('.');
			//truncate to mills
			if (i != -1 && i < val.length() - 4) {
				val = val.substring(0, i + 4);
			}
			buffer.append("datetime ").append('\"').append(val).append('\"'); //$NON-NLS-1$
		} else {
			super.visit(obj);
		}
	}
}
