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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.olingo.client.core.ConfigurationImpl;
import org.apache.olingo.client.core.uri.URIBuilderImpl;
import org.teiid.language.Condition;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ODataSelectQuery extends ODataQuery {
    private Integer skip;
    private Integer top;
    private boolean count;

    public ODataSelectQuery(ODataExecutionFactory executionFactory, RuntimeMetadata metadata) {
        super(executionFactory, metadata);
    }

    public URIBuilderImpl buildURL(String serviceRoot,
            List<Column> projectedColumns, Condition condition)
            throws TranslatorException {

        URIBuilderImpl uriBuilder = new URIBuilderImpl(new ConfigurationImpl(), serviceRoot);
        if (!this.rootDocument.isComplexType()) {
            uriBuilder.appendEntitySetSegment(this.rootDocument.getName());
        }

        if (this.count) {
            uriBuilder.count();
        } else {
            Set<String> columns = processSelect(projectedColumns);
            if (columns.isEmpty()) {
                uriBuilder.select(this.rootDocument.getTable().getPrimaryKey().getColumns().get(0).getName());
            } else {
                uriBuilder.select(columns.toArray(new String[columns.size()]));
            }
        }

        String filter = processFilter(condition);
        if (filter != null) {
            uriBuilder.filter(filter);
        }

        // process navigation tables
        for (ODataDocumentNode use:this.expandTables) {
            uriBuilder.expandWithOptions(use.getName(), use.getOptions());
        }

        if (this.skip != null) {
            uriBuilder.skip(this.skip);
        }

        if (this.top != null) {
            uriBuilder.top(this.top);
        }

        return uriBuilder;
    }

    private Set<String> processSelect(List<Column> projectedColumns) {
        LinkedHashSet<String> columns = new LinkedHashSet<String>();
        for (Column column: projectedColumns) {
            ODataDocumentNode use = getSchemaElement((Table)column.getParent());
            use.appendSelect(column.getName());
        }

        columns.addAll(this.rootDocument.getSelects());
        for (ODataDocumentNode use:this.complexTables) {
            columns.addAll(use.getSelects());
        }
        return columns;
    }

    public void setSkip(Integer integer) {
        this.skip = integer;
    }

    public void setTop(Integer integer) {
        this.top = integer;
    }

    public void setAsCount() {
        this.count = true;
    }
}
