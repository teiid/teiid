/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.google.visitor;

import java.util.HashMap;
import java.util.Map;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;

/**
 * Translates SQL INSERT commands
 * 
 * @author felias
 * 
 */
public class SpreadsheetInsertVisitor extends SQLStringVisitor {
	private String worksheetKey;
	private Map<String, String> columnNameValuePair;
	SpreadsheetInfo info;
	private String worksheetTitle;

	public SpreadsheetInsertVisitor(SpreadsheetInfo info) {
		this.info = info;
		columnNameValuePair = new HashMap<String, String>();
	}

	public void visit(Insert obj) {
		worksheetTitle = obj.getTable().getName();
		if (obj.getTable().getMetadataObject().getNameInSource() != null) {
			worksheetTitle = obj.getTable().getMetadataObject().getNameInSource();
		}
		worksheetKey = info.getWorksheetByName(worksheetTitle).getId();
		ExpressionValueSource evs = (ExpressionValueSource)obj.getValueSource();
		for (int i = 0; i < evs.getValues().size(); i++) {
		    Expression e = evs.getValues().get(i);
		    if (!(e instanceof Literal)) {
                throw new SpreadsheetOperationException("Only literals are allowed in the values section");
            }
		    Literal l = (Literal)e;
		    if (l.getValue() == null) {
		        continue;
	        }
	        Class<?> type = l.getType();
	        String value = null;
	        if (Number.class.isAssignableFrom(type)) {
	            value = l.getValue().toString();
	        } else if (type.equals(DataTypeManager.DefaultDataClasses.STRING)) {
	            value = "'"+l.getValue().toString();
	        } else {
	            value = l.getValue().toString();
	        }		
	        ColumnReference columnReference = obj.getColumns().get(i);
	        columnNameValuePair.put(columnReference.getMetadataObject().getSourceName(), value);
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
