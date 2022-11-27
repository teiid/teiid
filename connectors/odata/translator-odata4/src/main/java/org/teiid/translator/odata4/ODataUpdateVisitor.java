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
package org.teiid.translator.odata4;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.translator.TranslatorException;

public class ODataUpdateVisitor extends HierarchyVisitor {
    protected enum OperationType {INSERT, UPDATE, DELETE};
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private ODataUpdateQuery odataQuery;
    private RuntimeMetadata metadata;
    private OperationType operationType;

    public ODataUpdateVisitor(ODataExecutionFactory ef, RuntimeMetadata metadata) {
        this.odataQuery = new ODataUpdateQuery(ef, metadata);
        this.metadata = metadata;
    }

    public OperationType getOperationType() {
        return this.operationType;
    }

    public ODataUpdateQuery getODataQuery() {
        return this.odataQuery;
    }

    @Override
    public void visit(Insert obj) {
        this.operationType = OperationType.INSERT;
        visitNode(obj.getTable());

        try {
            // read the properties
            int elementCount = obj.getColumns().size();
            for (int i = 0; i < elementCount; i++) {
                Column column = obj.getColumns().get(i).getMetadataObject();
                List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();
                Expression expr = values.get(i);
                Object value = resolveExpressionValue(expr);
                this.odataQuery.addInsertProperty(column, ODataMetadataProcessor.getNativeType(column), value);
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    private Object resolveExpressionValue(Expression expr) throws TranslatorException {
        Object value = null;
        if (expr instanceof Literal) {
            value = ((Literal)expr).getValue();
        }
        else if (expr instanceof org.teiid.language.Array) {
            org.teiid.language.Array contents = (org.teiid.language.Array)expr;
            List<Expression> arrayExprs = contents.getExpressions();
            List<Object> values = new ArrayList<Object>();
            for (Expression exp:arrayExprs) {
                if (exp instanceof Literal) {
                    values.add(((Literal)exp).getValue());
                }
                else {
                    this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17029)));
                }
            }
            value = values;
        }
        else {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17029)));
        }
        return value;
    }


    @Override
    public void visit(Update obj) {
        this.operationType = OperationType.UPDATE;
        visitNode(obj.getTable());
        this.odataQuery.setCondition(obj.getWhere());

        try {
            // read the properties
            int elementCount = obj.getChanges().size();
            for (int i = 0; i < elementCount; i++) {
                Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
                String type = ODataTypeManager.odataType(column)
                        .getFullQualifiedName().getFullQualifiedNameAsString();
                Expression expr = obj.getChanges().get(i).getValue();
                Object value = resolveExpressionValue(expr);
                this.odataQuery.addUpdateProperty(column, type, value);
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    @Override
    public void visit(Delete obj) {
        this.operationType = OperationType.DELETE;
        visitNode(obj.getTable());
        this.odataQuery.setCondition(obj.getWhere());
    }

    @Override
    public void visit(NamedTable obj) {
        try {
            this.odataQuery.addRootDocument(obj.getMetadataObject());
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }
}
