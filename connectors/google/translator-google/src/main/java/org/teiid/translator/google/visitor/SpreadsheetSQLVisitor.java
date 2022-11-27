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

import static org.teiid.language.SQLConstants.Reserved.*;

import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
/**
 * Translates SQL SELECT queries
 *
 * @author felias
 *
 */
public class SpreadsheetSQLVisitor extends SpreadsheetCriteriaVisitor {

    private Integer limitValue = null;
    private Integer offsetValue = null;

    public SpreadsheetSQLVisitor(SpreadsheetInfo spreadsheetInfo) {
        super(spreadsheetInfo);
    }

    /**
     * Return only col name e.g. "A"
     */
    @Override
    protected String replaceElementName(String group, String element) {
        String columnId=worksheet.getColumnID(element);
         if(columnId==null){
             throw new SpreadsheetOperationException("Column "+element +" doesn't exist in the worksheet "+worksheet.getName());
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
            String name =table.getMetadataObject().getSourceName();
            setWorksheetByName(name);
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

    @Override
    public void visit(Like obj) {
        if (obj.isNegated()) {
            buffer.append("("); //$NON-NLS-1$
        }
        super.visit(obj);
        if (obj.isNegated()) {
            buffer.append(" AND "); //$NON-NLS-1$
            visitNode(obj.getLeftExpression());
            buffer.append(" IS NOT NULL)"); //$NON-NLS-1$
        }
    }

    @Override
    protected boolean isUpdate() {
        return false;
    }
}
