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

package org.teiid.olingo.service;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.olingo.ProjectedColumn;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;

public class ApplyDocumentNode extends DocumentNode {

    public static ApplyDocumentNode buildApplyDocumentNode(DocumentNode previousContext, UniqueNameGenerator nameGenerator, EdmStructuredType edmStructuredType) {
        ApplyDocumentNode resource = new ApplyDocumentNode(previousContext);

        String group = nameGenerator.getNextGroup();

        resource.setGroupSymbol(new GroupSymbol(group));
        resource.setEdmStructuredType(edmStructuredType);
        return resource;
    }


    private DocumentNode context;

    private GroupBy groupBy;

    public ApplyDocumentNode(DocumentNode context) {
        this.context = context;
    }

    DocumentNode getNestedContext() {
        return context;
    }

    @Override
    protected void addAllColumns(boolean onlyPK) {

    }

    @Override
    OrderBy addDefaultOrderBy() {
        return null;
    }

    @Override
    public Query buildQuery() {
        Query inner = this.context.buildQuery();
        inner.setGroupBy(groupBy);

        Query query = new Query();
        query.setSelect(new Select());

        if (this.projectedColumns.size() > 0) {
            AtomicInteger ordinal = new AtomicInteger(1);
            addColumns(query.getSelect(), ordinal, sortColumns(getProjectedColumns().values()));
        } else {
            query.getSelect().addSymbol(new MultipleElementSymbol());
        }

        query.setFrom(new From(Arrays.asList(new SubqueryFromClause(this.getGroupSymbol(), inner))));
        query.setCriteria(this.getCriteria());
        return query;
    }

    public DocumentNode getBaseContext() {
        if (context instanceof ApplyDocumentNode) {
            return ((ApplyDocumentNode)context).getBaseContext();
        }
        return this.context;
    }

    @Override
    public ContextColumn getColumnByName(String name) {
        if (this.projectedColumnsByName.size() > 0) {
            return this.projectedColumnsByName.get(name);
        }
        if (this.context.projectedColumnsByName.size() > 0) {
            return this.context.projectedColumnsByName.get(name);
        }
        return this.context.getColumnByName(name);
    }

    @Override
    public String getName() {
        return this.context.getName();
    }

    @Override
    public String toString() {
        return "Apply " + getGroupSymbol() + " " + context; //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public List<DocumentNode> getSiblings() {
        return context.getSiblings();
    }

    public void setGroupBy(GroupBy grouping) {
        this.groupBy = grouping;
    }

    @Override
    public List<ProjectedColumn> getAllProjectedColumns() {
        List<ProjectedColumn> all = super.getAllProjectedColumns();
        if (this.projectedColumns.size() == 0) {
            all.addAll(context.getAllProjectedColumns());
        }
        return all;
    }

    @Override
    Criteria buildJoinCriteria(DocumentNode joinResource,
            EdmNavigationProperty property) throws TeiidException {
        throw new TeiidRuntimeException("not implemented"); //$NON-NLS-1$
    }

    @Override
    public String getFullName() {
        throw new TeiidRuntimeException("not implemented"); //$NON-NLS-1$
    }

    @Override
    public void addSibling(DocumentNode resource) {
        throw new TeiidRuntimeException("not implemented"); //$NON-NLS-1$
    }

    @Override
    public FromClause getFromClause() {
        throw new TeiidRuntimeException("not implemented"); //$NON-NLS-1$
    }

}
