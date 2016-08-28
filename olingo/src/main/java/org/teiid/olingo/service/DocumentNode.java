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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.core.uri.queryoption.expression.LiteralImpl;
import org.teiid.core.TeiidException;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.ProjectedColumn;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;

public class DocumentNode {
    private Table table;
    private GroupSymbol groupSymbol;
    private EdmEntityType edmEntityType;
    private List<UriParameter> keyPredicates;
    private FromClause fromClause;
    private Criteria criteria;
    private LinkedHashMap<Expression, ProjectedColumn> projectedColumns = new LinkedHashMap<Expression, ProjectedColumn>();
    private List<DocumentNode> sibilings = new ArrayList<DocumentNode>();
    private List<ExpandDocumentNode> expands = new ArrayList<ExpandDocumentNode>();
    private boolean distinct;
	private DocumentNode iterator;
        
    public static DocumentNode build(EdmEntityType type,
            List<UriParameter> keyPredicates, MetadataStore metadata, OData odata,
            UniqueNameGenerator nameGenerator, boolean useAlias,
            UriInfo uriInfo, URLParseService parseService)
            throws TeiidException {
        DocumentNode resource = new DocumentNode();
        return build(resource, type, keyPredicates, metadata, odata,
                nameGenerator, useAlias, uriInfo, parseService);
    }
    
    public static DocumentNode build(DocumentNode resource,
            EdmEntityType type, List<UriParameter> keyPredicates,
            MetadataStore metadata, OData odata, UniqueNameGenerator nameGenerator,
            boolean useAlias, UriInfo uriInfo, URLParseService parseService)
            throws TeiidException {

        Table table = findTable(type, metadata);
        GroupSymbol gs = null;
        
        if (useAlias) {
            gs = new GroupSymbol(nameGenerator.getNextGroup(), table.getFullName()); //$NON-NLS-1$
        } else {
            gs = new GroupSymbol(table.getFullName()); //$NON-NLS-1$
        }

        resource.setTable(table);
        resource.setGroupSymbol(gs);
        resource.setEdmEntityType(type);
        resource.setKeyPredicates(keyPredicates);
        resource.setFromClause(new UnaryFromClause(gs));
        
        if (keyPredicates != null && !keyPredicates.isEmpty()) {
            Criteria criteria = DocumentNode.buildEntityKeyCriteria(resource,
                    uriInfo, metadata, odata, nameGenerator, parseService);
            resource.setCriteria(criteria);
        }        
        return resource;
    }    
    
    static Table findTable(EdmEntityType entityType, MetadataStore store) {
        FullQualifiedName fqn = entityType.getFullQualifiedName();
        // remove the vdb name
        String withoutVDB = fqn.getNamespace().substring(fqn.getNamespace().lastIndexOf('.')+1);
        Schema schema = store.getSchema(withoutVDB);
        return schema.getTable(entityType.getName());
    }    
    
    static Table findTable(EdmEntitySet entitySet, MetadataStore store) {
        return findTable(entitySet.getEntityType(), store);
    }

    static Criteria buildEntityKeyCriteria(DocumentNode resource,
            UriInfo uriInfo, MetadataStore store, OData odata, 
            UniqueNameGenerator nameGenerator, URLParseService parseService)
            throws TeiidException {
        
        KeyRecord pk = resource.getTable().getPrimaryKey();
        if (resource.getKeyPredicates().size() == 1) {
            if (pk.getColumns().size() != 1) {
                throw new TeiidException(ODataPlugin.Event.TEIID16015,
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, resource.getTable().getFullName()));
            }
            Column column = resource.getTable().getPrimaryKey().getColumns().get(0);
            
            ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                    resource, false, uriInfo, store, odata, nameGenerator, null, parseService);
            UriParameter key = resource.getKeyPredicates().get(0);
            org.apache.olingo.server.api.uri.queryoption.expression.Expression expr = getKeyPredicateExpression(
                    key, odata, column);
            return new CompareCriteria(new ElementSymbol(column.getName(),
                    resource.getGroupSymbol()), CompareCriteria.EQ,
                    visitor.getExpression(expr));
        }

        // complex (multi-keyed)
        List<Criteria> critList = new ArrayList<Criteria>();
        if (pk.getColumns().size() != resource.getKeyPredicates().size()) {
            throw new TeiidException(ODataPlugin.Event.TEIID16015,
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, resource.getTable().getFullName()));
        }
        for (UriParameter key : resource.getKeyPredicates()) {
            Column column = findColumn(resource.getTable(), key.getName());
            ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(
                    resource, false, uriInfo, store, odata, nameGenerator, null, parseService);
            org.apache.olingo.server.api.uri.queryoption.expression.Expression expr = getKeyPredicateExpression(
                    key, odata, column);            
            critList.add(new CompareCriteria(new ElementSymbol(column.getName(), resource.getGroupSymbol()), 
                    CompareCriteria.EQ, visitor.getExpression(expr)));
        }
        return new CompoundCriteria(CompoundCriteria.AND, critList);
    }

    private static org.apache.olingo.server.api.uri.queryoption.expression.Expression getKeyPredicateExpression(
            UriParameter key, OData odata, Column column) {        
        org.apache.olingo.server.api.uri.queryoption.expression.Expression expr = key.getExpression();
        if ( expr == null) {
            EdmPrimitiveTypeKind primitiveTypeKind = ODataTypeManager.odataType(column.getRuntimeType()); 
            expr = new LiteralImpl(key.getText(), odata.createPrimitiveTypeInstance(primitiveTypeKind));
        }
        return expr;
    }
    
    static Column findColumn(Table table, String propertyName) {
        return table.getColumnByName(propertyName);
    }
    
    public void buildEntityKeyCriteria(UriInfo uriInfo, MetadataStore metadata, OData odata,
            UniqueNameGenerator nameGenerator, URLParseService parseService) throws TeiidException {
        // URL is like /entitySet(key)s
        if (getKeyPredicates() != null && !getKeyPredicates().isEmpty()) {
            this.criteria = buildEntityKeyCriteria(this, uriInfo, metadata, odata, nameGenerator, parseService);
        }        
    }
    
    public DocumentNode() {
    }
    
    public DocumentNode(Table table, GroupSymbol gs, EdmEntityType type) {
        this.table = table;
        this.groupSymbol = gs;
        this.edmEntityType = type;        
    }
    
    private Table getTable() {
        return table;
    }
        
    public String getName() {
        return table.getName();
    }
    
    public Column getColumnByName(String name) {
        return this.table.getColumnByName(name);
    }

    public String getFullName() {
        return table.getFullName();
    }
    
    public GroupSymbol getGroupSymbol() {
        return groupSymbol;
    }
        
    public EdmEntityType getEdmEntityType() {
        return edmEntityType;
    }

    public FromClause getFromClause() {
        return fromClause;
    }

    public void setFromClause(FromClause fromClause) {
        this.fromClause = fromClause;
    }

    public Criteria getCriteria() {
        return criteria;
    }

    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setGroupSymbol(GroupSymbol groupSymbol) {
        this.groupSymbol = groupSymbol;
    }

    public void setEdmEntityType(EdmEntityType edmEntityType) {
        this.edmEntityType = edmEntityType;
    }
    
    protected void addAllColumns(boolean onlyPK) {
        if (onlyPK) {
            for (final Column column : getTable().getPrimaryKey().getColumns()) {
                if (column.isSelectable()) {
                    addProjectedColumn(column.getName(), new ElementSymbol(column.getName(), getGroupSymbol()));
                }
            }            
        }
        else {
            for (final Column column : getTable().getColumns()) {
                if (column.isSelectable()) {
                	addProjectedColumn(column.getName(), new ElementSymbol(column.getName(), getGroupSymbol()));
                }
            }
        }
    }

    protected void addProjectedColumn(final String columnName,
            final Expression expr) {
        EdmPropertyImpl edmProperty = (EdmPropertyImpl) this.edmEntityType.getProperty(columnName);
        Column c = getColumnByName(columnName);
        ProjectedColumn pc = addProjectedColumn(expr, edmProperty.getType(), edmProperty, edmProperty.isCollection());
        pc.setOrdinal(c.getPosition());
    }

    protected ProjectedColumn addProjectedColumn(final Expression expr, final EdmType type,
            EdmProperty property, final boolean collection) {
        ProjectedColumn pc = this.projectedColumns.get(expr);
        if (pc != null) {
        	return pc;
        }
        pc = new ProjectedColumn(expr, type, property, collection);
        pc.setOrdinal(Integer.MAX_VALUE);
        this.projectedColumns.put(expr, pc);
        return pc;
    }
    
    OrderBy addDefaultOrderBy() {
        if (this.table == null) {
            return null;
        }
        OrderBy orderBy = new OrderBy();
        // provide implicit ordering for cursor logic
        KeyRecord record = this.table.getPrimaryKey();
        if (record == null) {
            // if PK is not available there MUST at least one unique key
            record = this.table.getUniqueKeys().get(0);
        }
        // provide implicit ordering for cursor logic
        for (Column column:record.getColumns()) {
            ElementSymbol expr = new ElementSymbol(column.getName(), this.groupSymbol);
            orderBy.addVariable(expr);
            addProjectedColumn(column.getName(), expr);
        }
        return orderBy;
    }

    public LinkedHashMap<Expression, ProjectedColumn> getProjectedColumns() {
        return projectedColumns;
    }

    public List<ProjectedColumn> getAllProjectedColumns() {
        ArrayList<ProjectedColumn> columns = new ArrayList<ProjectedColumn>();
        columns.addAll(this.projectedColumns.values());
        for (DocumentNode er:this.sibilings) {
            columns.addAll(er.getAllProjectedColumns());
        }
        if (this.iterator != null) {
        	columns.addAll(this.iterator.getAllProjectedColumns());
        }
        return columns;
    }    
    
    public List<UriParameter> getKeyPredicates() {
        return keyPredicates;
    }

    public List<String> getKeyColumnNames(){
        return this.edmEntityType.getKeyPredicateNames();
    }
    
    public void setKeyPredicates(List<UriParameter> keyPredicates) {
        this.keyPredicates = keyPredicates;
    }
    
    public void addSibiling(DocumentNode resource) {
        this.sibilings.add(resource);
    }
    
    public List<DocumentNode> getSibilings(){
        return this.sibilings;
    }

    public void addExpand(ExpandDocumentNode resource) {
        this.expands.add(resource);
    }
    
    public List<ExpandDocumentNode> getExpands(){
        return this.expands;
    }
    
    public Query buildQuery() {
        
        Select select = new Select();
        AtomicInteger ordinal = new AtomicInteger(1);
        addProjectedColumns(select, ordinal, sortColumns(getProjectedColumns().values()));
        for (DocumentNode sibiling:this.sibilings) {
            addProjectedColumns(select, ordinal, sortColumns(sibiling.getProjectedColumns().values()));
        }
        select.setDistinct(this.distinct);

        Query query = new Query();
        From from = new From();
        
        from.addClause(this.fromClause);
        for (DocumentNode sibiling:this.sibilings) {
            from.addClause(sibiling.getFromClause());
        }
        if (this.iterator != null) {
        	addProjectedColumns(select, ordinal, sortColumns(this.iterator.getProjectedColumns().values()));
        	from.addClause(this.iterator.getFromClause());
        	GroupBy groupBy = new GroupBy();
        	for (String keyCol : this.getKeyColumnNames()) {
        		groupBy.addSymbol(new ElementSymbol(keyCol, this.groupSymbol));
        	}
        	query.setGroupBy(groupBy);
        }
        
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(this.criteria);
        
        return query;
    }

	private List<ProjectedColumn> sortColumns(Collection<ProjectedColumn> toSort) {
		//provide a stable sort of the columns regardless of visitation order.
		ArrayList<ProjectedColumn> list = new ArrayList<ProjectedColumn>(toSort);
		Collections.sort(list, new Comparator<ProjectedColumn>() {
        	@Override
        	public int compare(ProjectedColumn o1, ProjectedColumn o2) {
        		return Integer.compare(o1.getOrdinal(), o2.getOrdinal());
        	}
        });
		return list;
	}

    private void addProjectedColumns(Select select, AtomicInteger ordinal,
            List<ProjectedColumn> projected) {
        for (ProjectedColumn column:projected) {
            select.addSymbol(column.getExpression());
            column.setOrdinal(ordinal.getAndIncrement());
        }
    }
    
    DocumentNode joinTable(DocumentNode joinResource, boolean isCollection, JoinType joinType) throws TeiidException {
        ForeignKey fk = null;
        if (isCollection) {
            fk = joinFK(joinResource.getTable(), getTable());    
        }
        else {
            fk = joinFK(getTable(), joinResource.getTable());
        }
        
        // reverse lookup
        if (fk == null) {
            if (isCollection) {
                fk = joinFK(getTable(), joinResource.getTable());
            }
            else {
                fk = joinFK(joinResource.getTable(), getTable());
            }
        }
        
        if (fk == null && !joinType.equals(JoinType.JOIN_CROSS)) {
            throw new TeiidException("Fk not found");
        }
        
        FromClause fromClause;
        if (joinResource.getKeyPredicates() != null && joinResource.getKeyPredicates().size() > 0) {
            // here the previous entityset is verbose; need to be canonicalized
            fromClause = new UnaryFromClause(joinResource.getGroupSymbol());
        }
        else {
            fromClause = addJoinTable(joinType, joinResource, this);
        }
        
        joinResource.setFromClause(fromClause);        
        return joinResource;
    }
    
    static ForeignKey joinFK(DocumentNode current, DocumentNode reference) {
        Table currentTable = current.getTable();
        Table referenceTable = reference.getTable();
        if (currentTable ==  null || referenceTable == null) {
            return null;
        }
        
        for (ForeignKey fk : currentTable.getForeignKeys()) {
            String refSchemaName = fk.getReferenceKey().getParent().getParent().getName();
            if (referenceTable.getParent().getName().equals(refSchemaName)
                    && referenceTable.getName().equals(fk.getReferenceTableName())) {
                return fk;
            }
        }
        return null;
    }    

    private static ForeignKey joinFK(Table currentTable, Table referenceTable) {
        for (ForeignKey fk : currentTable.getForeignKeys()) {
            String refSchemaName = fk.getReferenceKey().getParent().getParent().getName();
            if (referenceTable.getParent().getName().equals(refSchemaName)
                    && referenceTable.getName().equals(fk.getReferenceTableName())) {
                return fk;
            }
        }
        return null;
    }
    
    private static FromClause addJoinTable(final JoinType joinType, DocumentNode from, DocumentNode to) {
        Criteria crit = null;
        if (!joinType.equals(JoinType.JOIN_CROSS)) {
            crit = buildJoinCriteria(from, to);
            if (crit == null) {
                crit = buildJoinCriteria(to, from);
            }
        }
        return new JoinPredicate(to.getFromClause(), new UnaryFromClause(from.getGroupSymbol()), joinType, crit);
    }

    static Criteria buildJoinCriteria(DocumentNode from, DocumentNode to) {
        Criteria criteria = null;
        for (ForeignKey fk:from.getTable().getForeignKeys()) {
            if (fk.getReferenceKey().getParent().equals(to.getTable())) {
                List<String> fkColumns = DocumentNode.getColumnNames(fk.getColumns());
                if (fkColumns == null) {
                    fkColumns = DocumentNode.getColumnNames(from.getTable().getPrimaryKey().getColumns());
                }                   
                
                List<String> pkColumns = DocumentNode.getColumnNames(to.getTable().getPrimaryKey().getColumns());
                criteria = DocumentNode.buildJoinCriteria(
                        from.getGroupSymbol(),
                        to.getGroupSymbol(), pkColumns, fkColumns);
            }
        } 
        return criteria;
    }
    
    static Criteria buildJoinCriteria(final GroupSymbol joinGroup,
            final GroupSymbol entityGroup, List<String> pkColumns,
            List<String> refColumns) {
        List<Criteria> critList = new ArrayList<Criteria>();

        for (int i = 0; i < refColumns.size(); i++) {
            critList.add(new CompareCriteria(new ElementSymbol(pkColumns.get(i), entityGroup), CompareCriteria.EQ, new ElementSymbol(refColumns.get(i), joinGroup)));
        }

        Criteria crit = critList.get(0);
        for (int i = 1; i < critList.size(); i++) {
            crit = new CompoundCriteria(CompoundCriteria.AND, crit, critList.get(i));
        }
        return crit;
    }

    static List<String> getColumnNames(List<Column> columns){
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        
        ArrayList<String> columnNames = new ArrayList<String>();
        for (Column column:columns) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    public void setDistinct(boolean b) {
        this.distinct = b;
    }
    
    public void addCriteria(Expression filter) {
        if (filter != null) {
        	Criteria crit = null;
        	if (filter instanceof Criteria) {
        		crit = (Criteria)filter;
        	} else {
        		crit = new ExpressionCriteria(filter);
        	}
        	this.criteria = Criteria.combineCriteria(this.criteria, crit);
        }
    }
    
    public String toString() {
        return table.getFullName();
    }

	public void setIterator(DocumentNode itResource) {
		this.iterator = itResource;
	}

	public DocumentNode getIterator() {
		return this.iterator;
	}
}
