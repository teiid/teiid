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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.client.api.uri.QueryOption;
import org.teiid.core.util.StringUtil;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.LanguageUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;
import org.teiid.translator.odata4.UriSchemaElement.SchemaElementType;

public class ODataQuery {
    protected ODataExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    protected UriSchemaElement entitySetTable;
    protected ArrayList<UriSchemaElement> complexTables = new ArrayList<UriSchemaElement>();
    protected ArrayList<UriSchemaElement> expandTables = new ArrayList<UriSchemaElement>();

    
    public ODataQuery(ODataExecutionFactory executionFactory, RuntimeMetadata metadata) {
        this.executionFactory = executionFactory;
        this.metadata = metadata;
    }

    public void addTable(Table table) {
        if (isComplexType(table)) {
            this.complexTables.add(new UriSchemaElement(table, SchemaElementType.COMPLEX, isCollection(table)));
        } else if (this.entitySetTable == null && isEntitySet(table)) {
            this.entitySetTable = new UriSchemaElement(table, SchemaElementType.PRIMARY, isCollection(table));
        } else {
            this.expandTables.add(new UriSchemaElement(table, SchemaElementType.EXPAND, isCollection(table)));
        }
    }
    
    protected void handleMissingEntitySet() throws TranslatorException {
        if (this.entitySetTable == null) {
            Table table = null;
            if (!this.complexTables.isEmpty()) {
                table = this.complexTables.get(0).getTable();
            } else if (!this.expandTables.isEmpty()) {
                table = this.expandTables.get(0).getTable();
            }
            String parentTable = table.getProperty(ODataMetadataProcessor.MERGE, false);
            if (parentTable == null) {
                throw new TranslatorException(ODataPlugin.Event.TEIID17028, 
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17028, table.getName()));
            }
            addTable(this.metadata.getTable(parentTable));
        }
    }    
    
    public UriSchemaElement getEntitySetTable() {
        return this.entitySetTable;
    }
    
    protected String processFilter(Condition condition) throws TranslatorException {
        List<Condition> crits = LanguageUtil.separateCriteriaByAnd(condition);
        if (!crits.isEmpty()) {
            for(Iterator<Condition> iter = crits.iterator(); iter.hasNext();) {
                Condition crit = iter.next();
                ODataFilterVisitor visitor = new ODataFilterVisitor(this.executionFactory, this.metadata, this);
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
    
    public Condition addNavigation(Condition obj, Table... tables) throws TranslatorException {
        for (Table table:tables) {
            addTable(table);    
        }       
        return parseKeySegmentFromCondition(obj);
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
    
    private boolean isCollection(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.ENTITY_COLLECTION
                || type == ODataType.COMPLEX_COLLECTION
                || type == ODataType.NAVIGATION_COLLECTION;
    }    
    
    private boolean isEntitySet(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.ENTITY_COLLECTION;
    }    
}

class UriSchemaElement {
    enum SchemaElementType {PRIMARY, COMPLEX, EXPAND};
    private Table table;
    private final SchemaElementType type; 
    private LinkedHashSet<String> columns = new LinkedHashSet<String>();
    private String filterStr;
    private boolean collection;
    
    public UriSchemaElement(Table t, SchemaElementType type, boolean collection) {
        this.table = t;
        this.type = type;
        this.collection = collection;
    }
    
    public Table getTable() {
        return this.table;
    }
    
    public String getName() {
        if (this.table.getNameInSource() != null) {
            return this.table.getNameInSource();
        }
        return this.table.getName();
    }
    
    boolean isCollection() {
        return this.collection;
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
    
    public void appendSelect(String columnName) {
        if (isComplexType()) {
            this.columns.add(getName());
        } else {
            this.columns.add(columnName);
        }
    }
    
    public Set<String> getSelects(){
        return this.columns;
    }
    
    public Map<QueryOption, Object> getOptions() {
        Map<QueryOption, Object> options = new LinkedHashMap<QueryOption, Object>();        
        
        if (isExpandType()) {
            if (!columns.isEmpty()) {
                options.put(QueryOption.SELECT, StringUtil.join(this.columns, ","));
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
    
    public List<String> getIdentityColumns(){
        if (this.table.getPrimaryKey() == null) {
            return Collections.emptyList();
        }
        ArrayList<String> keys = new ArrayList<String>();
        for (Column column:this.table.getPrimaryKey().getColumns()) {
            keys.add(column.getName());
        }
        return keys;
    }
}
