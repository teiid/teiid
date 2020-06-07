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

import java.util.LinkedHashMap;
import java.util.Map;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;

/**
 * Translates SQL INSERT commands
 *
 * @author felias
 *
 */
public class SpreadsheetInsertVisitor extends SpreadsheetCriteriaVisitor {
    private Map<String, Object> columnNameValuePair = new LinkedHashMap<String, Object>();

    public SpreadsheetInsertVisitor(SpreadsheetInfo info) {
        super(info);
    }

    public void visit(Insert obj) {
        setWorksheetByName(obj.getTable().getMetadataObject().getSourceName());
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
            ColumnReference columnReference = obj.getColumns().get(i);
            columnNameValuePair.put(columnReference.getMetadataObject().getSourceName(), l.getValue());
        }
    }

    public Map<String, Object> getColumnNameValuePair() {
        return columnNameValuePair;
    }

}
