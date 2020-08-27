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
package org.teiid.translator.salesforce.execution.visitors;

import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;


public class UpdateVisitor extends CriteriaVisitor implements IQueryProvidingVisitor {

    private Condition where;

    public UpdateVisitor(RuntimeMetadata metadata) {
        super(metadata);
    }

    @Override
    public void visit(Delete delete) {
        this.where = delete.getWhere();
        super.visit(delete);
        try {
            loadColumnMetadata(delete.getTable());
        } catch (TranslatorException ce) {
            exceptions.add(ce);
        }
    }

    @Override
    public void visit(Update update) {
        this.where = update.getWhere();
        // don't visit the changes or they will be in the query.
        visitNode(update.getTable());
        visitNode(update.getWhere());
        try {
            loadColumnMetadata(update.getTable());
        } catch (TranslatorException ce) {
            exceptions.add(ce);
        }
    }

    /*
     * The SOQL SELECT command uses the following syntax: SELECT fieldList FROM
     * objectType [WHERE The Condition Expression (WHERE Clause)] [ORDER BY]
     * LIMIT ?
     */

    public String getQuery() throws TranslatorException {
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
        StringBuilder result = new StringBuilder();
        result.append(SELECT).append(SPACE);
        result.append("Id").append(SPACE); //$NON-NLS-1$
        result.append(FROM).append(SPACE);
        result.append(table.getMetadataObject().getSourceName()).append(SPACE);
        addCriteriaString(result);
        return result.toString();
    }

    public Condition getWhere() {
        return where;
    }

}
