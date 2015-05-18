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
package org.teiid.olingo.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.commons.core.edm.primitivetype.EdmInt32;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoAll;
import org.apache.olingo.server.api.uri.UriInfoBatch;
import org.apache.olingo.server.api.uri.UriInfoCrossjoin;
import org.apache.olingo.server.api.uri.UriInfoEntityId;
import org.apache.olingo.server.api.uri.UriInfoMetadata;
import org.apache.olingo.server.api.uri.UriInfoService;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResourceAction;
import org.apache.olingo.server.api.uri.UriResourceComplexProperty;
import org.apache.olingo.server.api.uri.UriResourceCount;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceFunction;
import org.apache.olingo.server.api.uri.UriResourceIt;
import org.apache.olingo.server.api.uri.UriResourceLambdaAll;
import org.apache.olingo.server.api.uri.UriResourceLambdaAny;
import org.apache.olingo.server.api.uri.UriResourceLambdaVariable;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceRef;
import org.apache.olingo.server.api.uri.UriResourceRoot;
import org.apache.olingo.server.api.uri.UriResourceSingleton;
import org.apache.olingo.server.api.uri.UriResourceValue;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.FormatOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SearchOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.SkipTokenOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.apache.olingo.server.core.RequestURLHierarchyVisitor;
import org.apache.olingo.server.core.uri.parser.Parser;
import org.apache.olingo.server.core.uri.parser.UriParserException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.api.ODataTypeManager;
import org.teiid.olingo.api.ProjectedColumn;
import org.teiid.olingo.api.SQLParameter;
import org.teiid.query.sql.lang.AbstractCompareCriteria;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;

public class ODataSQLBuilder extends RequestURLHierarchyVisitor {
    private final MetadataStore metadata;
    private boolean prepared = true;
    private final Map<String, List<ProjectedColumn>> projectedColumnsByEntityType = new LinkedHashMap<String, List<ProjectedColumn>>();
    private final ArrayList<SQLParameter> params = new ArrayList<SQLParameter>();
    private FromClause fromClause;
    private Criteria criteria;
    private final ArrayList<TeiidException> exceptions = new ArrayList<TeiidException>();
    private Table edmEntityTable;
    private GroupSymbol edmEntityTableGroup;
    private EdmEntityType edmEntityType;
    private SkipOption skipOption;
    private TopOption topOption;
    private boolean countOption;
    private OrderBy orderBy;
    private boolean selectionComplete;
    private String nextToken;
    private boolean aliasedGroups;
    private boolean countQuery = false;
    private boolean reference = false;
    private String baseURI;
    private ServiceMetadata serviceMetadata;
    private UniqueNameGenerator nameGenerator = new UniqueNameGenerator();
    private boolean distinct = false;
    private ExpandOption expandOption;
    private ExpandInfo expandInfo;
    private ItResource itResource; 
    
    static class UniqueNameGenerator {
        private final AtomicInteger groupCount = new AtomicInteger(1);
        public String getNextGroup() {
            String aliasGroup = "g"+this.groupCount.getAndIncrement(); //$NON-NLS-1$
            return aliasGroup;
        }
    }
    
    static class ItResource {
        private ElementSymbol referencedProperty;
        private Table referencedTable;
        private ProjectedColumn projectedProperty;
        private FromClause projectedFromClause;
        private Criteria criteria;
        private EdmEntityType edmEntityType;
        private GroupSymbol groupSymbol;

        public void setReferencedProperty(ElementSymbol referencedProperty) {
            this.referencedProperty = referencedProperty;
        }
        
        public void setReferencedTable(Table referencedTable) {
            this.referencedTable = referencedTable;
        }

        public ElementSymbol getReferencedProperty() {
            return referencedProperty;
        }

        public Table getReferencedTable() {
            return referencedTable;
        }

        public ProjectedColumn getProjectedProperty() {
            return projectedProperty;
        }

        public FromClause getProjectedFromClause() {
            return projectedFromClause;
        }

        public void setProjectedProperty(ProjectedColumn projectedProperty) {
            this.projectedProperty = projectedProperty;
        }

        public void setProjectedFromClause(FromClause projectedFromClause) {
            this.projectedFromClause = projectedFromClause;
        }

        public Criteria getCriteria() {
            return criteria;
        }
        
        public void setCriteria(Criteria criteria) {
            this.criteria = criteria;
        }

        public void setEdmEntityType(EdmEntityType edmEntityType) {
            this.edmEntityType = edmEntityType;
        }

        public EdmEntityType getEdmEntityType() {
            return edmEntityType;
        }

        public GroupSymbol getProjectedGroup() {
            return this.groupSymbol;
        }
        
        public void setProjectedGroup(GroupSymbol groupSymbol) {
            this.groupSymbol = groupSymbol;
        }
    }
    
    public ODataSQLBuilder(MetadataStore metadata, boolean prepared,
            boolean aliasedGroups, String baseURI,
            ServiceMetadata serviceMetadata) {
        this.metadata = metadata;
        this.prepared = prepared;
        this.aliasedGroups = aliasedGroups;
        this.baseURI = baseURI;
        this.serviceMetadata = serviceMetadata;
    }

    public EdmEntityType getEdmEntityType() {
        return this.edmEntityType;
    }

    public boolean includeTotalSize() {
        return countOption;
    }

    public Integer getSkip() {
        if (skipOption == null) {
            return null;
        }
        return skipOption.getValue();
    }

    public Integer getTop() {
        if (topOption == null) {
            return null;
        }
        return topOption.getValue();
    }
    
    private ProjectedColumn replace$ItExpression(ProjectedColumn pc) {
        if (this.itResource != null
                && this.itResource.getReferencedProperty() != null
                && this.itResource.getReferencedProperty().equals(pc.getExpression())
                && itResource.getProjectedProperty() != null) {
            return itResource.getProjectedProperty();
        }
        return pc;
    }

    public Query selectQuery() throws TeiidException {
        
        if (this.expandOption != null) {
            processExpandOption(this.expandOption);
        }
        
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }

        int ordinal = 1;
        Select select = new Select();
        List<ProjectedColumn> projected = getProjectedColumns(this.edmEntityType);
        for (ProjectedColumn column:projected) {
            select.addSymbol(column.getExpression());
            column.setOrdinal(ordinal++);
        }
        if (this.expandInfo != null) {
            projected = getProjectedColumns(this.expandInfo.getEntityType());
            for (ProjectedColumn column:projected) {
                select.addSymbol(column.getExpression());
                column.setOrdinal(ordinal++);
            }
        }
        select.setDistinct(this.distinct);

        Query query = new Query();
        From from = new From();
        from.addClause(this.fromClause);
        if (this.itResource != null && this.itResource.getProjectedFromClause() != null) {
            from.addClause(this.itResource.getProjectedFromClause());
        }
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(this.criteria);

        if (this.countQuery) {
            AggregateSymbol aggregateSymbol = new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null);
            select = new Select(Arrays.asList(aggregateSymbol));
            query.setSelect(select);
        }

        if (this.orderBy != null & !countQuery) {
            query.setOrderBy(this.orderBy);
        }
        return query;
    }
    
    public ExpandInfo getExpandInfo() {
        return this.expandInfo;
    }

    private void processExpandOption(ExpandOption option) {
        if (option.getExpandItems().size() > 1) {
            this.exceptions.add(new TeiidException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16042)));
            return;
        }
        for (ExpandItem ei:option.getExpandItems()) {
            
            ExpandSQLBuilder esb = new ExpandSQLBuilder(ei);
            
            EdmNavigationProperty property = esb.getNavigationProperty();
            EdmEntityType entityType = property.getType();
            
            String aliasGroup = nameGenerator.getNextGroup();
            Table expandTable = findTable(entityType, this.metadata);
            GroupSymbol expandGroup = new GroupSymbol(aliasGroup, expandTable.getFullName());
            
            joinTable(entityType, expandTable, expandGroup, null, property.isCollection(), JoinType.JOIN_LEFT_OUTER);
            
            this.itResource = new ItResource();
            this.itResource.setReferencedTable(expandTable);
            
            // process $filter
            if (ei.getFilterOption() != null) {
                Criteria expandCriteria = processFilterOption(ei.getFilterOption(), expandTable, expandGroup);
                if (expandCriteria != null) {
                    if (this.criteria == null) {
                        this.criteria = expandCriteria;
                    }
                    else {
                        this.criteria = new CompoundCriteria(CompoundCriteria.AND, this.criteria, expandCriteria);
                    }
                }
            }
            
            if (ei.getOrderByOption() != null) {
                if (this.orderBy == null) {
                    this.orderBy = new OrderBy();
                }
                processOrderBy(this.orderBy, ei.getOrderByOption().getOrders(), expandTable, expandGroup, entityType);
            }
            
            // process $select
            processSelectOption(ei.getSelectOption(), expandTable, expandGroup, entityType);
            
            this.expandInfo = new ExpandInfo(property.getName(), entityType,
                    getProjectedColumns(entityType), property.isCollection());
            
            if (ei.getSkipOption() != null 
                    || ei.getCountOption() != null
                    || ei.getTopOption() != null
                    || ei.getLevelsOption() != null) {
                this.exceptions.add(new TeiidException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16041)));
            }
        }
    }

    private Criteria processFilterOption(FilterOption option, Table table, GroupSymbol groupSymbol) {
        ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                table, groupSymbol, this.prepared,
                getUriInfo(), this.metadata, this.nameGenerator, this.itResource, this.params);
        Criteria filterCriteria = null;
        try {
            filterCriteria = (Criteria)visitor.getExpression(option.getExpression());
            if (visitor.getLambda() != null) {
                this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16025, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16025)));
            }
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
        return filterCriteria;
    }

    public List<ProjectedColumn> getProjectedColumns(EdmEntityType type) {
        List<ProjectedColumn> columns = this.projectedColumnsByEntityType.get(type.getName());
        List<ProjectedColumn> modified = new ArrayList<ProjectedColumn>();
        for (ProjectedColumn pc: columns) {
            modified.add(replace$ItExpression(pc));
        }
        return modified;
    }

    static Table findTable(EdmEntitySet entitySet, MetadataStore store) {
        return findTable(entitySet.getEntityType(), store);
    }

    static Table findTable(EdmEntityType entityType, MetadataStore store) {
        FullQualifiedName fqn = entityType.getFullQualifiedName();
        // remove the vdb name
        String withoutVDB = fqn.getNamespace().substring(fqn.getNamespace().lastIndexOf('.')+1);
        Schema schema = store.getSchema(withoutVDB);
        return schema.getTable(entityType.getName());
    }

    static Column findColumn(Table table, String propertyName) {
        return table.getColumnByName(propertyName);
    }

    public List<SQLParameter> getParameters(){
        return this.params;
    }

    @Override
    public void visit(UriResourceEntitySet info) {
        this.edmEntityTable = findTable(info.getEntitySet(), this.metadata);
        this.edmEntityType = info.getEntitySet().getEntityType();
        if (aliasedGroups) {
            this.edmEntityTableGroup = new GroupSymbol("g0", this.edmEntityTable.getFullName()); //$NON-NLS-1$
        } else {
            this.edmEntityTableGroup = new GroupSymbol(this.edmEntityTable.getFullName()); //$NON-NLS-1$
        }
        
        this.fromClause = new UnaryFromClause(this.edmEntityTableGroup);

        // URL is like /entitySet(key)s
        if (info.getKeyPredicates() != null && !info.getKeyPredicates().isEmpty()) {
            List<UriParameter> keys = info.getKeyPredicates();
            try {
                this.criteria = buildEntityKeyCriteria(this.edmEntityTable,
                        this.edmEntityTableGroup, keys, getUriInfo(),
                        this.metadata, this.nameGenerator);
            } catch (TeiidException e) {
                this.exceptions.add(e);
            }
        }
        this.itResource = new ItResource();
        this.itResource.setReferencedTable(this.edmEntityTable);
    }

    static Criteria buildEntityKeyCriteria(Table table, GroupSymbol tableGroup,
            List<UriParameter> keys, UriInfo uriInfo, MetadataStore store,
            UniqueNameGenerator nameGenerator) throws TeiidException {
        
        KeyRecord pk = table.getPrimaryKey();
        if (keys.size() == 1) {
            if (pk.getColumns().size() != 1) {
                throw new TeiidException(ODataPlugin.Event.TEIID16015,
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015,table.getFullName()));
            }
            Column column = table.getPrimaryKey().getColumns().get(0);
            
            ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                    table, tableGroup, false, uriInfo, store, nameGenerator, null, null);

            return new CompareCriteria(new ElementSymbol(column.getName(),
                    tableGroup), CompareCriteria.EQ, visitor.getExpression(keys.get(0).getExpression()));
        }

        // complex (multi-keyed)
        List<Criteria> critList = new ArrayList<Criteria>();
        if (pk.getColumns().size() != keys.size()) {
            throw new TeiidException(ODataPlugin.Event.TEIID16015,
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, table.getFullName()));
        }
        for (UriParameter key : keys) {
            Column column = findColumn(table, key.getName());
            ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                    table, tableGroup, false, uriInfo, store, nameGenerator, null, null);
            critList.add(new CompareCriteria(new ElementSymbol(column.getName(), tableGroup), 
                    CompareCriteria.EQ, visitor.getExpression(key.getExpression())));
        }
        return new CompoundCriteria(CompoundCriteria.AND, critList);
    }

    @Override
    public void visit(SkipOption option) {
        this.skipOption = option;
    }

    @Override
    public void visit(TopOption option) {
        this.topOption = option;
    }

    @Override
    public void visit(CountOption info) {
        this.countOption = info.getValue();
    }

    @Override
    public void visit(SelectOption option) {
        if (this.selectionComplete) {
            return;
        }
        processSelectOption(option, this.edmEntityTable, this.edmEntityTableGroup, this.edmEntityType);
    }

    private void processSelectOption(SelectOption option, Table table, GroupSymbol groupSymbol, EdmEntityType type) {
        if (option == null) {
            // default select columns
            addAllColumns(table, groupSymbol, type);
        }
        else {
            for (SelectItem si:option.getSelectItems()) {
                if (si.isStar()) {
                    addAllColumns(table, groupSymbol, type);
                    continue;
                }

                try {
                    ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                            table, groupSymbol, false,
                            getUriInfo(), this.metadata, this.nameGenerator, this.itResource, this.params);
                    ElementSymbol expr = (ElementSymbol)visitor.getExpression(si.getResourcePath());
                    addVisibleColumn(expr.getShortName(), expr, type);
                } catch (TeiidException e) {
                    this.exceptions.add(e);
                }
            }
        }
    }

    private void addAllColumns(Table table, GroupSymbol gs, final EdmEntityType type) {
        if (this.reference) {
            for (final Column column : table.getPrimaryKey().getColumns()) {
                addVisibleColumn(column.getName(), new ElementSymbol(column.getName(), gs), type);
            }            
        }
        else {
            for (final Column column : table.getColumns()) {
                addVisibleColumn(column.getName(), new ElementSymbol(column.getName(), gs), type);
            }
        }
    }

    private void addVisibleColumn(final String columnName, final Expression expr, final EdmEntityType entityType) {
        addEntityColumn(columnName, expr, true, entityType);
    }
    
    private void addEntityColumn(final String columnName,
            final Expression expr, boolean visibility,
            final EdmEntityType entityType) {
        EdmPropertyImpl edmProperty = (EdmPropertyImpl)  entityType.getProperty(columnName);
        addProjectedColumn(columnName, expr, visibility, edmProperty.getType(), edmProperty.isCollection(), entityType);
    }    

    private void addProjectedColumn(final String columnName, final Expression expr,
            final boolean visibility, final EdmType type, final boolean collection, final EdmEntityType entityType) {
        
        List<ProjectedColumn> projected = this.projectedColumnsByEntityType.get(entityType.getName());
        if (projected == null) {
            projected = new ArrayList<ProjectedColumn>();
            this.projectedColumnsByEntityType.put(entityType.getName(), projected);
        }
        
        int i = 0;
        for (i = 0; i < projected.size(); i++) {
            ProjectedColumn pc = projected.get(i);
            if (pc.getExpression().equals(expr)) {
                projected.remove(i);
                break;
            }
        }
        projected.add(new ProjectedColumn(expr, visibility, type, collection));
    }

    @Override
    public void visit(OrderByOption option) {
        this.orderBy = new OrderBy();

        if (option == null || option.getOrders().isEmpty()) {
            // provide implicit ordering for cursor logic
            KeyRecord record = this.edmEntityTable.getPrimaryKey();
            if (record == null) {
                // if PK is not available there MUST at least one unique key
                record = this.edmEntityTable.getUniqueKeys().get(0);
            }
            // provide implicit ordering for cursor logic
            for (Column column:record.getColumns()) {
                ElementSymbol expr = new ElementSymbol(column.getName(), this.edmEntityTableGroup);
                this.orderBy.addVariable(expr);
                addEntityColumn(column.getName(), expr, false, this.edmEntityType);
            }
        }
        else {
            List<OrderByItem> orderBys = option.getOrders();
            processOrderBy(this.orderBy, orderBys, this.edmEntityTable, this.edmEntityTableGroup, this.edmEntityType);
        }
    }

    private void processOrderBy(OrderBy orderBy, List<OrderByItem> orderByItems, Table table, GroupSymbol gs, EdmEntityType edmType) {
        for (OrderByItem orderby:orderByItems) {
            ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                    table, gs, false,
                    getUriInfo(), this.metadata, this.nameGenerator, this.itResource, this.params);
            try {
                Expression expr = visitor.getExpression(orderby.getExpression());
                if (expr instanceof ElementSymbol) {
                    orderBy.addVariable(expr, !orderby.isDescending());
                    addEntityColumn(((ElementSymbol)expr).getShortName(), expr, false, edmType);
                }
                else {
                    AliasSymbol alias = new AliasSymbol("_orderByAlias", expr);
                    orderBy.addVariable(alias, !orderby.isDescending());
                    addProjectedColumn("_orderByAlias", alias, false, EdmInt32.getInstance(), false, edmType);
                }
            } catch (TeiidException e) {
                this.exceptions.add(e);
            }
        }
    }

    @Override
    public void visit(FilterOption info) {
        ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                this.edmEntityTable, this.edmEntityTableGroup, this.prepared,
                getUriInfo(), this.metadata, this.nameGenerator, this.itResource, this.params);
        
        Criteria filterCriteria = null;
        try {
             filterCriteria = (Criteria)visitor.getExpression(info.getExpression());
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
        
        if (visitor.getLambda() != null) {
            Lambda lambda = visitor.getLambda();
            if (lambda.getKind() == Lambda.Kind.ANY) {
                // olingo does not correctly reflect collection on lambda variable
                joinTable(lambda.getType(), lambda.getTable(), lambda.getGroupSymbol(), null, 
                        (joinFK(this.edmEntityTable, lambda.getTable()) == null), JoinType.JOIN_INNER);
                
//                this.edmEntityTableGroup = lambda.getGroupSymbol();
//                this.edmEntityTable = lambda.getTable();
//                this.edmEntityType = lambda.getType();                
                
                this.distinct = true;
            }
        }
        
        this.criteria = filterCriteria;
    }

    static ForeignKey joinFK(Table currentTable, Table referenceTable) {
        for (ForeignKey fk : currentTable.getForeignKeys()) {
            String refSchemaName = fk.getReferenceKey().getParent().getParent().getName();
            if (referenceTable.getParent().getName().equals(refSchemaName)
                    && referenceTable.getName().equals(fk.getReferenceTableName())) {
                return fk;
            }
        }
        return null;
    }
    
    @Override
    public void visit(UriResourceNavigation info) {
        EdmNavigationProperty property = info.getProperty();
        EdmEntityType joinTableType = property.getType();
        
        String aliasGroup = nameGenerator.getNextGroup();
        Table joinTable = findTable(joinTableType, this.metadata);
        GroupSymbol joinGroup = new GroupSymbol(aliasGroup, joinTable.getFullName());
        
        joinTable(joinTableType, joinTable, joinGroup, info.getKeyPredicates(), property.isCollection(), JoinType.JOIN_INNER);
        this.edmEntityTableGroup = joinGroup;
        this.edmEntityTable = joinTable;
        this.edmEntityType = joinTableType;
        this.itResource = new ItResource();
        this.itResource.setReferencedTable(this.edmEntityTable);
    }

    private void joinTable(EdmEntityType joinTableType, Table joinTable,
            GroupSymbol joinGroup, List<UriParameter> keys, boolean isCollection, JoinType joinType) {
        
        ForeignKey fk = null;
        if (isCollection) {
            fk = joinFK(joinTable, this.edmEntityTable);    
        }
        else {
            fk = joinFK(this.edmEntityTable, joinTable);
        }
        
        if (fk == null) {
            this.exceptions.add(new TeiidException("Fk not found"));
            return;
        }
        
        List<String> refColumns = fk.getReferenceColumns();
        if (refColumns == null) {
            refColumns = getColumnNames(this.edmEntityTable.getPrimaryKey().getColumns());
        }

        try {
            if (keys != null && keys.size() > 0) {
                // here the previous entityset is verbose; need to be canonicalized
                this.criteria = buildEntityKeyCriteria(joinTable, joinGroup,
                        keys, getUriInfo(), this.metadata, this.nameGenerator);
                   this.fromClause = new UnaryFromClause(joinGroup);
            }
            else {
                this.fromClause = addJoinTable(joinType, joinGroup,
                        this.edmEntityTableGroup, refColumns,
                        getColumnNames(fk.getColumns()));
            }
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
    }

    private FromClause addJoinTable(final JoinType joinType,
            final GroupSymbol joinGroup, final GroupSymbol entityGroup, List<String> pkColumns,
            List<String> refColumns) {

        List<Criteria> critList = new ArrayList<Criteria>();

        for (int i = 0; i < refColumns.size(); i++) {
            critList.add(new CompareCriteria(new ElementSymbol(pkColumns.get(i), entityGroup), CompareCriteria.EQ, new ElementSymbol(refColumns.get(i), joinGroup)));
        }

        Criteria crit = critList.get(0);
        for (int i = 1; i < critList.size(); i++) {
            crit = new CompoundCriteria(CompoundCriteria.AND, crit, critList.get(i));
        }
        return new JoinPredicate(this.fromClause, new UnaryFromClause(joinGroup), joinType, crit);
    }

    static List<String> getColumnNames(List<Column> columns){
        ArrayList<String> columnNames = new ArrayList<String>();
        for (Column column:columns) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    @Override
    public void visit(UriResourcePrimitiveProperty info) {
        String propertyName = info.getProperty().getName();
        ElementSymbol es = new ElementSymbol(propertyName, this.edmEntityTableGroup);
        addVisibleColumn(propertyName, es, this.edmEntityType);
        
        this.itResource = new ItResource();
        this.itResource.setReferencedTable(this.edmEntityTable);
        this.itResource.setReferencedProperty(es);
        this.selectionComplete = true;
    }
    
    public String getNextToken() {
        return nextToken;
    }
    
    @Override
    public void visit(SkipTokenOption option) {
        if (option != null) {
            this.nextToken = option.getValue();
        }
    }
    
    @Override
    public void visit(SearchOption option) {
        this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16035, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16035)));
    }
    
    public Insert insert(EdmEntityType entityType, Entity entity, boolean prepared) {
        Table entityTable = findTable(entityType.getName(), this.metadata);
        this.edmEntityTable = entityTable;
        this.edmEntityType = entityType;
        this.edmEntityTableGroup = new GroupSymbol(entityTable.getFullName());
        
        List<Reference> referenceValues = new ArrayList<Reference>();
        List<Constant> constantValues = new ArrayList<Constant>();
        Insert insert = new Insert();
        insert.setGroup(this.edmEntityTableGroup);
        
        int i = 0;
        for (Property prop : entity.getProperties()) {
            EdmProperty edmProp = (EdmProperty)entityType.getProperty(prop.getName());
            Column column = entityTable.getColumnByName(edmProp.getName());
            insert.addVariable(new ElementSymbol(column.getName(), this.edmEntityTableGroup));
            
            if (prepared) {
                referenceValues.add(new Reference(i++));
                this.params.add(asParam(edmProp, prop.getValue()));
            }
            else {
                constantValues.add(new Constant(asParam(edmProp, prop.getValue()).getValue()));
            }
        }
        
        if (prepared) {
            insert.setValues(referenceValues);
        }
        else {
            insert.setValues(constantValues);
        }
        return insert;
    }
    
    static SQLParameter asParam(EdmProperty edmProp, Object value) {
        String teiidType = ODataTypeManager.teiidType((SingletonPrimitiveType)edmProp.getType(), edmProp.isCollection());
        int sqlType = JDBCSQLTypeInfo.getSQLType(teiidType);
        if (value == null) {
            return new SQLParameter(null, sqlType);
        }
        return new SQLParameter(ODataTypeManager.convertToTeiidRuntimeType(DataTypeManager.getDataTypeClass(teiidType), value), sqlType);
    }    
    
    private Table findTable(String tableName, MetadataStore store) {
        int idx = tableName.indexOf('.');
        if (idx > 0) {
            Schema s = store.getSchema(tableName.substring(0, idx));
            if (s != null) {
                Table t = s.getTable(tableName.substring(idx+1));
                if (t != null) {
                    return t;
                }
            }
        }
        for (Schema s : store.getSchemaList()) {
            Table t = s.getTables().get(tableName);
            if (t != null) {
                return t;
            }
        }        
        return null;
    }
    //TODO: allow the generated key building.
    public Query  selectWithEntityKey(EdmEntityType entityType, Entity entity, Map<String, Object> generatedKeys) {
        Table table = findTable(entityType.getName(), this.metadata);
        GroupSymbol tableGroup = new GroupSymbol(table.getFullName());
        
        addAllColumns(table, tableGroup, entityType);
        
        Select select = new Select();
        int ordinal = 1;
        for (ProjectedColumn pc : getProjectedColumns(entityType)) {
            select.addSymbol(pc.getExpression());
            pc.setOrdinal(ordinal++);
        }

        Query query = new Query();
        From from = new From();
        from.addClause(new UnaryFromClause(tableGroup));
        query.setSelect(select);
        query.setFrom(from);
    
        Criteria criteria = null;
        KeyRecord pk = table.getPrimaryKey();
        for (Column c:pk.getColumns()) {
            Property prop = entity.getProperty(c.getName());
            Constant right = null;
            if (prop != null) {
                right = new Constant(ODataTypeManager.convertToTeiidRuntimeType(c.getJavaType(), prop.getValue()));
            } else {
                Object value = generatedKeys.get(c.getName());
                if (value == null) {
                    // I observed with mysql did not return the label for column, 
                    // this may be workaround in single key case in compound case 
                    // we got to error.
                    if (pk.getColumns().size() == 1 && generatedKeys.size() == 1) {
                        value = generatedKeys.values().iterator().next();
                    }                    
                    if (value == null) {
                        throw new TeiidRuntimeException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16016, entityType.getName()));
                    }
                }
                right = new Constant(value);
            }
            ElementSymbol left = new ElementSymbol(c.getName(), tableGroup);
            if (criteria == null) {
                criteria = new CompareCriteria(left,AbstractCompareCriteria.EQ, right);
            }
            else {
                CompareCriteria rightCC = new CompareCriteria(left,AbstractCompareCriteria.EQ, right);
                criteria = new CompoundCriteria(CompoundCriteria.AND, criteria, rightCC);
            }
        }
        query.setCriteria(criteria);
        return query;
    }
    
    public Update update(EdmEntityType entityType, Entity entity, boolean prepared) {
        Update update = new Update();
        update.setGroup(this.edmEntityTableGroup);
        
        int i = 0;
        for (Property property : entity.getProperties()) {
            EdmProperty edmProperty = (EdmProperty)entityType.getProperty(property.getName());
            Column column = this.edmEntityTable.getColumnByName(edmProperty.getName());
            ElementSymbol symbol = new ElementSymbol(column.getName(), this.edmEntityTableGroup);
            boolean add = true;
            for (Column c : this.edmEntityTable.getPrimaryKey().getColumns()) {
                if (c.getName().equals(column.getName())) {
                    add = false;
                    break;
                }
            }
            if (add) {
                if (prepared) {
                    update.addChange(symbol, new Reference(i++));
                    this.params.add(asParam(edmProperty, property.getValue()));
                }
                else {
                    update.addChange(symbol, new Constant(asParam(edmProperty, property.getValue()).getValue()));
                }
            }
        }
        update.setCriteria(this.criteria);
        return update;
    }
    
    public Update updateProperty(EdmProperty edmProperty, Property property, boolean prepared) {
        Update update = new Update();
        update.setGroup(this.edmEntityTableGroup);

        Column column = this.edmEntityTable.getColumnByName(edmProperty.getName());
        ElementSymbol symbol = new ElementSymbol(column.getName(), this.edmEntityTableGroup);
        
        if (prepared) {
            update.addChange(symbol, new Reference(0));
            this.params.add(asParam(edmProperty, property.getValue()));
        }
        else {
            update.addChange(symbol, new Constant(asParam(edmProperty, property.getValue()).getValue()));
        }
        
        update.setCriteria(this.criteria);
        return update;
    }
    
    public Update updateStreamProperty(EdmProperty edmProperty, final InputStream content) {
        Update update = new Update();
        update.setGroup(this.edmEntityTableGroup);

        Column column = this.edmEntityTable.getColumnByName(edmProperty.getName());
        ElementSymbol symbol = new ElementSymbol(column.getName(), this.edmEntityTableGroup);
        
        update.addChange(symbol, new Reference(0));
        Class<?> lobType = DataTypeManager.getDataTypeClass(column.getDatatype().getRuntimeTypeName());
        int sqlType = JDBCSQLTypeInfo.getSQLType(column.getDatatype().getRuntimeTypeName());
        if (content == null) {
            this.params.add(new SQLParameter(null, sqlType));
        } else {
            Object value = null;
            InputStreamFactory isf = new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return content;
                }
            };
            
            if (lobType.isAssignableFrom(SQLXML.class)) {
                value = new SQLXMLImpl(isf);    
            } else if (lobType.isAssignableFrom(Clob.class)) {
                value = new ClobImpl(isf, -1);
            } else if (lobType.isAssignableFrom(Blob.class)) {
                value = new BlobImpl(isf);
            } else {
                this.exceptions.add(new TeiidException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16031, column.getName())));
            }
            this.params.add(new SQLParameter(value, sqlType));
        }
        update.setCriteria(this.criteria);
        return update;
    }
    
    public Delete delete() {
        Delete delete = new Delete();
        delete.setGroup(this.edmEntityTableGroup);
        delete.setCriteria(this.criteria);
        return delete;
    }    
    
    @Override
    public void visit(UriInfoService info) {
        this.exceptions.add(new TeiidException("UriInfoService NotSupported"));
    }

    @Override
    public void visit(UriInfoAll info) {
        this.exceptions.add(new TeiidException("UriInfoAll NotSupported"));
    }

    @Override
    public void visit(UriInfoBatch info) {
        this.exceptions.add(new TeiidException("UriInfoBatch NotSupported"));
    }

    @Override
    public void visit(UriInfoCrossjoin info) {
        this.exceptions.add(new TeiidException("UriInfoCrossjoin NotSupported"));
    }

    @Override
    public void visit(UriInfoMetadata info) {
        this.exceptions.add(new TeiidException("UriInfoMetadata NotSupported"));
    }

    @Override
    public void visit(ExpandOption option) {
        this.expandOption = option;
    }

    @Override
    public void visit(FormatOption info) {
    }

    @Override
    public void visit(UriInfoEntityId info) {
        
        try {
            visit(buildUriInfo(new URI(info.getIdOption().getValue()), this.baseURI, this.serviceMetadata));
        } catch (UriParserException e) {
            this.exceptions.add(new TeiidException(e));
        } catch (URISyntaxException e) {
            this.exceptions.add(new TeiidException(e));
        }
        
        visit(info.getSelectOption());

        if (info.getExpandOption() != null) {
            visit(info.getExpandOption());
        }
        if (info.getFormatOption() != null) {
            visit(info.getFormatOption());
        }
    }

    static UriInfo buildUriInfo(URI uri, String baseUri,
            ServiceMetadata serviceMetadata) throws URISyntaxException,
            UriParserException {        
        URI servicePath = new URI(baseUri);
        String path = servicePath.getPath();
        
        String rawPath = uri.getPath();
        int e = rawPath.indexOf(path);
        if (-1 == e) {
          rawPath = uri.getPath();
        } else {
          rawPath = rawPath.substring(e+path.length());
        }
        return new Parser().parseUri(rawPath, uri.getQuery(), null, serviceMetadata.getEdm());            
    }
    
    @Override
    public void visit(UriResourceCount option) {
        if (option!= null) {
            this.countQuery = true;
        }
    }

    @Override
    public void visit(UriResourceRef info) {
        this.reference = true;
    }

    @Override
    public void visit(UriResourceRoot info) {
        this.exceptions.add(new TeiidException("UriResourceRoot NotSupported"));
    }

    @Override
    public void visit(UriResourceValue info) {
    }

    @Override
    public void visit(UriResourceAction info) {
        this.exceptions.add(new TeiidException("UriResourceAction NotSupported"));
    }

    @Override
    public void visit(UriResourceFunction info) {
        this.exceptions.add(new TeiidException("UriResourceFunction NotSupported"));
    }

    @Override
    public void visit(UriResourceIt info) {
        this.exceptions.add(new TeiidException("UriResourceIt NotSupported"));
    }

    @Override
    public void visit(UriResourceLambdaAll info) {
        this.exceptions.add(new TeiidException("UriResourceLambdaAll NotSupported"));
    }

    @Override
    public void visit(UriResourceLambdaAny info) {
        this.exceptions.add(new TeiidException("UriResourceLambdaAll NotSupported"));
    }

    @Override
    public void visit(UriResourceLambdaVariable info) {
        this.exceptions.add(new TeiidException("UriResourceLambdaVariable NotSupported"));
    }

    @Override
    public void visit(UriResourceSingleton info) {
        this.exceptions.add(new TeiidException("UriResourceSingleton NotSupported"));
    }

    @Override
    public void visit(UriResourceComplexProperty info) {
        this.exceptions.add(new TeiidException("UriResourceComplexProperty NotSupported"));
    }
}
