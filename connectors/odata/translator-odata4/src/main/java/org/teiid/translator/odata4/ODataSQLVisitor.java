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

import static org.teiid.language.SQLConstants.Reserved.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.client.core.uri.URIBuilderImpl;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ODataSQLVisitor extends HierarchyVisitor {

    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected ODataExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    protected ArrayList<Column> projectedColumns = new ArrayList<Column>();
    private ODataSelectQuery odataQuery;
    private StringBuilder orderBy = new StringBuilder();
    private ArrayList<Condition> conditionFragments = new ArrayList<Condition>();

    public ODataSQLVisitor(ODataExecutionFactory executionFactory,
            RuntimeMetadata metadata) {
        this.executionFactory = executionFactory;
        this.metadata = metadata;
        this.odataQuery = new ODataSelectQuery(executionFactory, metadata);
    }

    public List<Column> getProjectedColumns(){
        return this.projectedColumns;
    }

    public ODataSelectQuery getODataQuery() {
        return this.odataQuery;
    }

    public String buildURL(String serviceRoot) throws TranslatorException {

        URIBuilderImpl uriBuilder = this.odataQuery.buildURL(serviceRoot,
                this.projectedColumns,
                LanguageUtil.combineCriteria(this.conditionFragments));

        if (this.orderBy.length() > 0) {
            uriBuilder.orderBy(this.orderBy.toString());
        }

        URI uri = uriBuilder.build();
        return uri.toString();
    }

    List<String> getColumnNames(List<Column> columns) {
        ArrayList<String> names = new ArrayList<String>();
        for (Column c : columns) {
            names.add(c.getName());
        }
        return names;
    }

    @Override
    public void visit(NamedTable obj) {
        try {
            this.odataQuery.addRootDocument(obj.getMetadataObject());
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    @Override
    public void visit(Join obj) {
        // joins are not used currently
        if (obj.getLeftItem() instanceof Join) {
            Condition updated = obj.getCondition();
            append(obj.getLeftItem());
            Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
            try {
                updated = this.odataQuery.addNavigation(obj.getCondition(), obj.getJoinType(), right);
                obj.setCondition(updated);
                if (updated != null) {
                    this.conditionFragments.add(obj.getCondition());
                }
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (obj.getRightItem() instanceof Join) {
            Condition updated = obj.getCondition();
            append(obj.getRightItem());
            Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
            try {
                updated = this.odataQuery.addNavigation(obj.getCondition(), obj.getJoinType(), left);
                obj.setCondition(updated);
                if (updated != null) {
                    this.conditionFragments.add(obj.getCondition());
                }
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else {
            Condition updated = obj.getCondition();
            Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
            Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
            try {
                if (ODataMetadataProcessor.isComplexType(left) ||
                        ODataMetadataProcessor.isNavigationType(left)) {
                    throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17027, left.getName()));
                }
                updated = this.odataQuery.addNavigation(obj.getCondition(), obj.getJoinType(), left, right);
                obj.setCondition(updated);
                if (updated != null) {
                    this.conditionFragments.add(obj.getCondition());
                }
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
    }

    @Override
    public void visit(Limit obj) {
        if (obj.getRowOffset() != 0) {
            this.odataQuery.setSkip(new Integer(obj.getRowOffset()));
        }
        if (obj.getRowLimit() != 0) {
            this.odataQuery.setTop(new Integer(obj.getRowLimit()));
        }
    }

    @Override
    public void visit(OrderBy obj) {
         append(obj.getSortSpecifications());
    }

    @Override
    public void visit(SortSpecification obj) {
        if (this.orderBy.length() > 0) {
            this.orderBy.append(Tokens.COMMA);
        }
        ColumnReference column = (ColumnReference)obj.getExpression();
        try {
            Column c = ODataMetadataProcessor.normalizePseudoColumn(this.metadata, column.getMetadataObject());
            this.orderBy.append(c.getName());
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
        // default is ascending
        if (obj.getOrdering() == Ordering.DESC) {
            this.orderBy.append(Tokens.SPACE).append(DESC.toLowerCase());
        }
    }

    @Override
    public void visit(Select obj) {
        visitNodes(obj.getFrom());
        this.conditionFragments.add(obj.getWhere());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());
        visitNodes(obj.getDerivedColumns());
    }

    @Override
    public void visit(DerivedColumn obj) {
        if (obj.getExpression() instanceof ColumnReference) {
            Column column = ((ColumnReference)obj.getExpression()).getMetadataObject();
            if (!column.isSelectable()) {
                this.exceptions.add(new TranslatorException(ODataPlugin.Util
                        .gs(ODataPlugin.Event.TEIID17006, column.getName())));
            }
            try {
                this.projectedColumns.add(ODataMetadataProcessor.normalizePseudoColumn(this.metadata, column));
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (obj.getExpression() instanceof AggregateFunction) {
            AggregateFunction func = (AggregateFunction)obj.getExpression();
            if (func.getName().equalsIgnoreCase("COUNT")) { //$NON-NLS-1$
                this.odataQuery.setAsCount();
            }
            else {
                this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17007, func.getName())));
            }
        }
        else {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17008)));
        }
    }

    public void append(LanguageObject obj) {
        visitNode(obj);
    }

    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            for (int i = 0; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }

    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            for (int i = 0; i < items.length; i++) {
                append(items[i]);
            }
        }
    }
}
