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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Expression;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.UpdateSet;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;

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
        setWorksheetByName(obj.getTable().getMetadataObject().getSourceName());
        changes = new ArrayList<UpdateSet>();
        String columnName;
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
