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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.client.api.uri.QueryOption;
import org.apache.olingo.client.core.ConfigurationImpl;
import org.apache.olingo.client.core.uri.URIBuilderImpl;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.LanguageUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;
import org.teiid.translator.odata4.UriSchemaElement.SchemaElementType;

public class ODataQuery {
    private ODataExecutionFactory executionFactory;
    private UriSchemaElement entitySetTable;
    private ArrayList<UriSchemaElement> complexTables = new ArrayList<UriSchemaElement>();
    private ArrayList<UriSchemaElement> expandTables = new ArrayList<UriSchemaElement>();
    private Integer skip;
    private Integer top;
    private boolean count;
    
    public ODataQuery(ODataExecutionFactory executionFactory) {
        this.executionFactory = executionFactory;
    }

    public void addTable(Table table) {
        if (isComplexType(table)) {
            this.complexTables.add(new UriSchemaElement(table, SchemaElementType.COMPLEX));
        } else if (this.entitySetTable == null && isEntitySet(table)) {
            this.entitySetTable = new UriSchemaElement(table, SchemaElementType.PRIMARY);
        } else {
            this.expandTables.add(new UriSchemaElement(table, SchemaElementType.EXPAND));
        }
    }
    
    public UriSchemaElement getEntitySetTable() {
        return this.entitySetTable;
    }
    
    public Condition addNavigation(Condition obj, Table... tables) throws TranslatorException {
        for (Table table:tables) {
            addTable(table);    
        }       
        return parseKeySegmentFromCondition(obj);
    }
    
    public URIBuilderImpl buildURL(String serviceRoot,
            ArrayList<Column> projectedColumns, Condition condition)
            throws TranslatorException {
        
        if(this.entitySetTable == null) {
            StringBuilder sb = new StringBuilder();
            for (UriSchemaElement use: this.complexTables) {
                if (sb.length() != 0) {
                    sb.append(",");
                }
                sb.append(use.getTable().getName());
            }
            for (UriSchemaElement use: this.expandTables) {
                if (sb.length() != 0) {
                    sb.append(",");
                }                
                sb.append(use.getTable().getName());                
            }
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17027, sb));
        }
        
        URIBuilderImpl uriBuilder = new URIBuilderImpl(new ConfigurationImpl(), serviceRoot);
        this.entitySetTable.appendEntitySetSegment(uriBuilder);
        
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

    private Set<String> processSelect(ArrayList<Column> projectedColumns) {
        LinkedHashSet<String> columns = new LinkedHashSet<String>();
        for (Column column: projectedColumns) {
            UriSchemaElement use = getSchemaElement((Table)column.getParent());
            use.appendSelect(column);
        }
        
        columns.addAll(this.entitySetTable.getSelects());        
        for (UriSchemaElement use:this.complexTables) {
            columns.addAll(use.getSelects());
        }
        return columns;
    }

    private String processFilter(Condition condition) throws TranslatorException {
        List<Condition> crits = LanguageUtil.separateCriteriaByAnd(condition);
        if (!crits.isEmpty()) {
            for(Iterator<Condition> iter = crits.iterator(); iter.hasNext();) {
                Condition crit = iter.next();
                ODataFilterVisitor visitor = new ODataFilterVisitor(this.executionFactory, this);
                visitor.appendFilter(crit);
            }
        }
        StringBuilder sb = new StringBuilder();
        if (this.entitySetTable.getFilter() != null) {
            sb.append(this.entitySetTable.getFilter());
        }
        for (UriSchemaElement use:this.complexTables) {
            if (use.getFilter() != null) {
                if (sb.length() > 0) {
                    sb.append(" and ");
                }
                sb.append(use.getFilter());
            }
        }
        return sb.length() == 0?null:sb.toString();
    }

    protected Condition parseKeySegmentFromCondition(Condition obj) throws TranslatorException {
        List<Condition> crits = LanguageUtil.separateCriteriaByAnd(obj);
        if (!crits.isEmpty()) {
            boolean modified = false;
            for(Iterator<Condition> iter = crits.iterator(); iter.hasNext();) {
                Condition crit = iter.next();
                if (crit instanceof Comparison) {
                    Comparison left = (Comparison) crit;
                    boolean leftAdded = parseKeySegmentFromComparison(left);
                    if (leftAdded) {
                        iter.remove();
                        modified = true;
                    }
                }
            }
            if (modified) {
                return LanguageUtil.combineCriteria(crits);
            }
        }
        return obj;
    }   
    
    private boolean parseKeySegmentFromComparison(Comparison obj) throws TranslatorException {
        if (obj.getOperator().equals(Comparison.Operator.EQ)) {
            if (obj.getLeftExpression() instanceof ColumnReference
                    && obj.getRightExpression() instanceof ColumnReference) {
                Column left = ((ColumnReference)obj.getLeftExpression()).getMetadataObject();
                Column right = ((ColumnReference)obj.getRightExpression()).getMetadataObject();

                if (isJoinOrPkColumn(left) && isJoinOrPkColumn(right)) {
                    // in odata the navigation from parent to child implicit by their keys
                    return true;
                }
            }
        }
        return false;
    }
    
    UriSchemaElement getSchemaElement(Table table) {
        if (this.entitySetTable != null && this.entitySetTable.getTable().equals(table)) {
            return this.entitySetTable;
        }
        for (UriSchemaElement schemaElement:this.complexTables) {
            if (schemaElement.getTable().equals(table)) {
                return schemaElement;
            }
        }
        for (UriSchemaElement schemaElement:this.expandTables) {
            if (schemaElement.getTable().equals(table)) {
                return schemaElement;
            }
        }        
        return null;
    }    

    private boolean isJoinOrPkColumn(Column column) {
        Table table = (Table)column.getParent();
        boolean isKey = (table.getPrimaryKey().getColumnByName(column.getName()) != null);
        if (!isKey) {
            for(ForeignKey fk:table.getForeignKeys()) {
                if (fk.getColumnByName(column.getName()) != null) {
                    isKey = true;
                }
            }
        }
        return isKey;
    }
    
    
    private boolean isComplexType(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.COMPLEX || type == ODataType.COMPLEX_COLLECTION;
    }
    
    private boolean isEntitySet(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.ENTITYSET;
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

class UriSchemaElement {
    enum SchemaElementType {PRIMARY, COMPLEX, EXPAND};
    private Table table;
    private final SchemaElementType type; 
    private LinkedHashSet<String> columns = new LinkedHashSet<String>();
    private String filterStr;
    
    public UriSchemaElement(Table t, SchemaElementType type) {
        this.table = t;
        this.type = type;
    }
    
    public Table getTable() {
        return this.table;
    }

    public void appendEntitySetSegment(URIBuilderImpl uriBuilder) {
        if (!isComplexType()) {
            uriBuilder.appendEntitySetSegment(getName());
        }
    }
    
    public String getName() {
        if (this.table.getNameInSource() != null) {
            return this.table.getNameInSource();
        }
        return this.table.getName();
    }
    
    boolean isComplexType() {
        return type == SchemaElementType.COMPLEX;
    }
    
    boolean isExpandType() {
        return type == SchemaElementType.EXPAND;
    }

    boolean isPrimaryType() {
        return type == SchemaElementType.PRIMARY;
    }
    
    public void appendSelect(Column column) {
        if (isComplexType()) {
            this.columns.add(getName());
        } else {
            this.columns.add(column.getName());
        }
    }
    
    public Set<String> getSelects(){
        return this.columns;
    }
    
    public Map<QueryOption, Object> getOptions() {
        Map<QueryOption, Object> options = new LinkedHashMap<QueryOption, Object>();        
        
        if (isExpandType()) {
            if (!columns.isEmpty()) {
                options.put(QueryOption.SELECT, StringUtils.join(
                        this.columns.toArray(new String[this.columns.size()]), ","));
            }
            if (this.filterStr != null) {
                options.put(QueryOption.FILTER, this.filterStr);
            }
        }
        return options;
    }

    public void addFilter(String string) {
        if (this.filterStr == null) {
            this.filterStr = string;
        } else {
            this.filterStr = this.filterStr +" and " + string; //$NON-NLS-1$
        }
    }
    
    public String getFilter() {
        return this.filterStr;
    }
}
