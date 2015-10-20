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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
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
import org.apache.olingo.server.core.uri.UriResourceEntitySetImpl;
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
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.odata.api.SQLParameter;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.ODataTypeManager;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;
import org.teiid.query.sql.lang.AbstractCompareCriteria;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.Insert;
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
    private final ArrayList<SQLParameter> params = new ArrayList<SQLParameter>();
    private final ArrayList<TeiidException> exceptions = new ArrayList<TeiidException>();
    private DocumentNode context;
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
    private UniqueNameGenerator nameGenerator;
    private ExpandOption expandOption;
    private URLParseService parseService;
        
    class URLParseService {
        public Query parse(String rawPath) throws TeiidException {
            try {
                rawPath = rawPath.replace("$root", "");
                UriInfo uriInfo = new Parser().parseUri(rawPath, null, null, serviceMetadata.getEdm());                
                ODataSQLBuilder visitor = new ODataSQLBuilder(metadata,
                        prepared, aliasedGroups, baseURI, serviceMetadata,
                        nameGenerator) {
                    public void visit(OrderByOption option) {
                        //no implicit ordering now.
                    }
                };
                visitor.visit(uriInfo);
                return visitor.selectQuery();
            } catch (UriParserException e) {
                throw new TeiidException(e);
            }
        }    
    }    
    
    public ODataSQLBuilder(MetadataStore metadata, boolean prepared,
            boolean aliasedGroups, String baseURI,
            ServiceMetadata serviceMetadata, UniqueNameGenerator nameGenerator) {
        this.metadata = metadata;
        this.prepared = prepared;
        this.aliasedGroups = aliasedGroups;
        this.baseURI = baseURI;
        this.serviceMetadata = serviceMetadata;
        this.nameGenerator = nameGenerator;
        this.parseService = new URLParseService();
    }

    public DocumentNode getContext() {
        return this.context;
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

    public Query selectQuery() throws TeiidException {
        
        if (this.expandOption != null) {
            processExpandOption(this.expandOption);
        }
        
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }

        Query query = this.context.buildQuery();
        if (this.countQuery) {
            AggregateSymbol aggregateSymbol = new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null);
            Select select = new Select(Arrays.asList(aggregateSymbol));
            query.setSelect(select);
        }

        if (this.orderBy != null & !countQuery) {
            query.setOrderBy(this.orderBy);
        }
        return query;
    }
    
    private void processExpandOption(ExpandOption option) {
        if (option.getExpandItems().size() > 1) {
            this.exceptions.add(new TeiidException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16042)));
            return;
        }
        for (ExpandItem ei:option.getExpandItems()) {
            
            try {
                ExpandSQLBuilder esb = new ExpandSQLBuilder(ei);
                EdmNavigationProperty property = esb.getNavigationProperty();
                ExpandDocumentNode expandResource = ExpandDocumentNode.buildExpand(
                        property, this.metadata, this.nameGenerator, true,
                        getUriInfo(), this.parseService);
                
                this.context.joinTable(expandResource, property.isCollection(), JoinType.JOIN_LEFT_OUTER);
                
                // process $filter
                if (ei.getFilterOption() != null) {
                    Criteria expandCriteria = processFilterOption(ei.getFilterOption(), expandResource);
                    expandResource.addCriteria(expandCriteria);
                    
                }
                
                this.context.addCriteria(expandResource.getCriteria());
                this.context.setFromClause(expandResource.getFromClause());
                
                if (ei.getOrderByOption() != null) {
                    if (this.orderBy == null) {
                        this.orderBy = new OrderBy();
                    }
                    processOrderBy(this.orderBy, ei.getOrderByOption().getOrders(), expandResource);
                }
                
                // process $select
                processSelectOption(ei.getSelectOption(), expandResource, this.reference);
                
                this.context.addExpand(expandResource);
            } catch (TeiidException e) {
                this.exceptions.add(e);
            }
            
            if (ei.getSkipOption() != null 
                    || ei.getCountOption() != null
                    || ei.getTopOption() != null
                    || ei.getLevelsOption() != null) {
                this.exceptions.add(new TeiidException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16041)));
            }
        }
    }

    private Criteria processFilterOption(FilterOption option, DocumentNode resource) {
        ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                resource, this.prepared, getUriInfo(), this.metadata,
                this.nameGenerator, this.params, this.parseService);
        Criteria filterCriteria = null;
        try {
            filterCriteria = (Criteria)visitor.getExpression(option.getExpression());
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
        return filterCriteria;
    }

    public List<SQLParameter> getParameters(){
        return this.params;
    }

    @Override
    public void visit(UriResourceEntitySet info) {
        try {
            this.context = DocumentNode.build(
                    info.getEntitySet().getEntityType(), info.getKeyPredicates(),
                    this.metadata, this.nameGenerator, this.aliasedGroups, getUriInfo(), this.parseService);
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
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
        processSelectOption(option, this.context, this.reference);
    }

    private void processSelectOption(SelectOption option, DocumentNode resource, boolean onlyReference) {
        if (option == null) {
            // default select columns
            resource.addAllColumns(onlyReference);
        }
        else {
            for (SelectItem si:option.getSelectItems()) {
                if (si.isStar()) {
                    resource.addAllColumns(onlyReference);
                    continue;
                }

                try {
                    ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                            resource, false,
                            getUriInfo(), this.metadata, this.nameGenerator, this.params,
                            this.parseService);
                    ElementSymbol expr = (ElementSymbol)visitor.getExpression(si.getResourcePath());
                    resource.addVisibleColumn(expr.getShortName(), expr);
                } catch (TeiidException e) {
                    this.exceptions.add(e);
                }
            }
        }
    }

    @Override
    public void visit(OrderByOption option) {        
        if (option == null || option.getOrders().isEmpty()) {
            this.orderBy = this.context.addDefaultOrderBy();
        }
        else {
            List<OrderByItem> orderBys = option.getOrders();
            this.orderBy = processOrderBy(new OrderBy(), orderBys, this.context);
        }
    }

    private OrderBy processOrderBy(OrderBy orderBy, List<OrderByItem> orderByItems, DocumentNode resource) {
        for (OrderByItem obitem:orderByItems) {
            ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                    resource, false,
                    getUriInfo(), this.metadata, this.nameGenerator, this.params,
                    this.parseService);
            try {
                Expression expr = visitor.getExpression(obitem.getExpression());
                if (expr instanceof ElementSymbol) {
                    orderBy.addVariable(expr, !obitem.isDescending());
                    visitor.getExpresionEntityResource().addProjectedColumn(((ElementSymbol)expr).getShortName(), expr, false);
                }
                else {
                    AliasSymbol alias = new AliasSymbol("_orderByAlias", expr);
                    orderBy.addVariable(alias, !obitem.isDescending());
                    visitor.getExpresionEntityResource().addProjectedColumn(alias, false, EdmInt32.getInstance(), false);
                }
            } catch (TeiidException e) {
                this.exceptions.add(e);
            }
        }
        return orderBy;
    }

    @Override
    public void visit(FilterOption info) {
        ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                this.context, this.prepared, getUriInfo(), this.metadata,
                this.nameGenerator, this.params, this.parseService);
        
        Criteria filterCriteria = null;
        try {
             filterCriteria = (Criteria)visitor.getExpression(info.getExpression());
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
        
        // Here Lambda operation may have joined a table and changed the context.
        this.context = visitor.getEntityResource();
        this.context.addCriteria(filterCriteria);
    }
    
    @Override
    public void visit(UriResourceNavigation info) {
        EdmNavigationProperty property = info.getProperty();
        try {
            DocumentNode joinResource = DocumentNode.build(property.getType(),
                    info.getKeyPredicates(), this.metadata, this.nameGenerator,
                    true, getUriInfo(), parseService);

            this.context.joinTable(joinResource, property.isCollection(), JoinType.JOIN_INNER);
            // In the context of canonical queries if key predicates are available then do not set the criteria 
            if (joinResource.getCriteria() == null) {
                joinResource.addCriteria(this.context.getCriteria());
            }
            this.context = joinResource;
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
    }

    @Override
    public void visit(UriResourcePrimitiveProperty info) {
        String propertyName = info.getProperty().getName();
        ElementSymbol es = new ElementSymbol(propertyName, this.context.getGroupSymbol());
        this.context.addVisibleColumn(propertyName, es);
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
    
    public Insert insert(EdmEntityType entityType, Entity entity, boolean prepared) throws TeiidException {
        Table entityTable = findTable(entityType.getName(), this.metadata);
        DocumentNode resource = new DocumentNode(entityTable, new GroupSymbol(entityTable.getFullName()), entityType);
        
        List<Reference> referenceValues = new ArrayList<Reference>();
        List<Constant> constantValues = new ArrayList<Constant>();
        Insert insert = new Insert();
        insert.setGroup(resource.getGroupSymbol());
        
        int i = 0;
        for (Property prop : entity.getProperties()) {
            EdmProperty edmProp = (EdmProperty)entityType.getProperty(prop.getName());
            Column column = entityTable.getColumnByName(edmProp.getName());
            insert.addVariable(new ElementSymbol(column.getName(), resource.getGroupSymbol()));
            
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
    
    static SQLParameter asParam(EdmProperty edmProp, Object value) throws TeiidException {
        String teiidType = ODataTypeManager.teiidType((SingletonPrimitiveType)edmProp.getType(), edmProp.isCollection());
        int sqlType = JDBCSQLTypeInfo.getSQLType(teiidType);
        if (value == null) {
            return new SQLParameter(null, sqlType);
        }
        return new SQLParameter(ODataTypeManager.convertToTeiidRuntimeType(
                DataTypeManager.getDataTypeClass(teiidType), value), sqlType);
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
    public Query selectWithEntityKey(EdmEntityType entityType, Entity entity,
            Map<String, Object> generatedKeys, Set<EdmNavigationProperty> expand) throws TeiidException {
        Table table = findTable(entityType.getName(), this.metadata);

        DocumentNode resource = new DocumentNode(table, new GroupSymbol(table.getFullName()), entityType);
        resource.setFromClause(new UnaryFromClause(new GroupSymbol(table.getFullName())));
        resource.addAllColumns(false);
        this.context = resource;
        
        FromClause from = resource.getFromClause();
        Criteria criteria = null;
        
        for (EdmNavigationProperty navProperty: expand) {
            DocumentNode joinResource = ExpandDocumentNode.buildExpand(
                    navProperty, this.metadata, this.nameGenerator,
                    this.aliasedGroups, getUriInfo(), this.parseService);
                    
            resource.joinTable(joinResource, navProperty.isCollection(), JoinType.JOIN_INNER);
            // In the context of canonical queries if key predicates are available then do not set the criteria 
            if (joinResource.getCriteria() == null) {
                joinResource.addCriteria(resource.getCriteria());
            }
            joinResource.addAllColumns(false);
            resource = joinResource;
            this.context.addExpand(joinResource);
            from = resource.getFromClause();
            criteria = resource.getCriteria();
        }
        
        this.context.setFromClause(from);
        Query query = this.context.buildQuery();

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
            ElementSymbol left = new ElementSymbol(c.getName(), this.context.getGroupSymbol());
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
    
    public Update update(EdmEntityType entityType, Entity entity, boolean prepared) throws TeiidException {
        Update update = new Update();
        update.setGroup(this.context.getGroupSymbol());
        
        int i = 0;
        for (Property property : entity.getProperties()) {
            EdmProperty edmProperty = (EdmProperty)entityType.getProperty(property.getName());
            Column column = this.context.getTable().getColumnByName(edmProperty.getName());
            ElementSymbol symbol = new ElementSymbol(column.getName(), this.context.getGroupSymbol());
            boolean add = true;
            for (Column c : this.context.getTable().getPrimaryKey().getColumns()) {
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
        update.setCriteria(this.context.getCriteria());
        return update;
    }
    
    public Update updateProperty(EdmProperty edmProperty, Property property,
            boolean prepared) throws TeiidException {
        Update update = new Update();
        update.setGroup(this.context.getGroupSymbol());

        Column column = this.context.getTable().getColumnByName(edmProperty.getName());
        ElementSymbol symbol = new ElementSymbol(column.getName(), this.context.getGroupSymbol());
        
        if (prepared) {
            update.addChange(symbol, new Reference(0));
            this.params.add(asParam(edmProperty, property.getValue()));
        }
        else {
            update.addChange(symbol, new Constant(asParam(edmProperty, property.getValue()).getValue()));
        }
        
        update.setCriteria(this.context.getCriteria());
        return update;
    }
    
    public Update updateStreamProperty(EdmProperty edmProperty, final InputStream content) {
        Update update = new Update();
        update.setGroup(this.context.getGroupSymbol());

        Column column = this.context.getTable().getColumnByName(edmProperty.getName());
        ElementSymbol symbol = new ElementSymbol(column.getName(), this.context.getGroupSymbol());
        
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
        update.setCriteria(this.context.getCriteria());
        return update;
    }
    
    public Delete delete() {
        Delete delete = new Delete();
        delete.setGroup(this.context.getGroupSymbol());
        delete.setCriteria(this.context.getCriteria());
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
        for (String name:info.getEntitySetNames()) {
            EdmEntitySet entitySet = this.serviceMetadata.getEdm().getEntityContainer().getEntitySet(name);
            EdmEntityType entityType = entitySet.getEntityType();
            CrossJoinNode resource = null;
            try {
                boolean hasExpand = hasExpand(entitySet.getName(), info.getExpandOption());
                resource = CrossJoinNode.buildCrossJoin(entityType, null,
                        this.metadata, this.nameGenerator, this.aliasedGroups,
                        getUriInfo(), this.parseService, hasExpand);
                resource.addAllColumns(!hasExpand);
                
                if (this.context == null) {
                    this.context = resource;                    
                    this.orderBy = this.context.addDefaultOrderBy();                    
                }
                else {
                    this.context.addSibiling(resource);
                    OrderBy orderby = resource.addDefaultOrderBy();
                    int index = orderby.getVariableCount();
                    for (int i = 0; i < index; i++) {
                        this.orderBy.addVariable(orderby.getVariable(i));
                    }
                }
            } catch (TeiidException e) {
                this.exceptions.add(e);
            }
        }
        super.visit(info);

        // the expand behavior is handled above with selection of the columns 
        this.expandOption = null;
    }
    
    private boolean hasExpand(String name, ExpandOption expandOption) {
        if (expandOption == null) {
            return false;
        }
        for (ExpandItem ei:expandOption.getExpandItems()) {
            String expand = ((UriResourceEntitySetImpl)ei.getResourcePath().getUriResourceParts().get(0)).getEntitySet().getName();
            if (expand.equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
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
