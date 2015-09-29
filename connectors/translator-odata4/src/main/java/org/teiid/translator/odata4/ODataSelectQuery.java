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
        
        handleMissingEntitySet();
        
        URIBuilderImpl uriBuilder = new URIBuilderImpl(new ConfigurationImpl(), serviceRoot);
        if (!this.entitySetTable.isComplexType()) {
            uriBuilder.appendEntitySetSegment(this.entitySetTable.getName());
        }
        
        if (this.count) {
            uriBuilder.count();
        } else {
            Set<String> columns = processSelect(projectedColumns);
            if (columns.isEmpty()) {
                uriBuilder.select(this.entitySetTable.getTable().getPrimaryKey().getColumns().get(0).getName());
            } else {
                uriBuilder.select(columns.toArray(new String[columns.size()]));
            }
        }
        
        String filter = processFilter(condition);
        if (filter != null) {
            uriBuilder.filter(filter);
        }

        // process navigation tables
        for (UriSchemaElement use:this.expandTables) {
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
            UriSchemaElement use = getSchemaElement((Table)column.getParent());
            use.appendSelect(column.getName());
        }
        
        columns.addAll(this.entitySetTable.getSelects());        
        for (UriSchemaElement use:this.complexTables) {
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
