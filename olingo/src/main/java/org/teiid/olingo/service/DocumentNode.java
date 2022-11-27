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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
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

    public interface ContextColumn {

        int getOrdinal();

        String getName();

        String getRuntimeType();

        EdmPrimitiveTypeKind getEdmPrimitiveTypeKind();

    }

    public static class TableContextColumn implements ContextColumn {

        private Column column;

        public TableContextColumn(Column c) {
            this.column = c;
        }

        @Override
        public int getOrdinal() {
            return column.getPosition();
        }

        @Override
        public String getName() {
            return column.getName();
        }

        @Override
        public String getRuntimeType() {
            return column.getRuntimeType();
        }

        @Override
        public EdmPrimitiveTypeKind getEdmPrimitiveTypeKind() {
            return ODataTypeManager.odataType(column);
        }



    }

    private Table table;
    private GroupSymbol groupSymbol;
    private EdmStructuredType edmStructuredType;
    private List<UriParameter> keyPredicates;
    private FromClause fromClause;
    private Criteria criteria;
    protected LinkedHashMap<Expression, ProjectedColumn> projectedColumns = new LinkedHashMap<Expression, ProjectedColumn>();
    protected LinkedHashMap<String, ProjectedColumn> projectedColumnsByName = new LinkedHashMap<String, ProjectedColumn>();
    private List<DocumentNode> siblings = new ArrayList<DocumentNode>();
    private List<ExpandDocumentNode> expands = new ArrayList<ExpandDocumentNode>();
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
            gs = new GroupSymbol(nameGenerator.getNextGroup(), table.getFullName());
        } else {
            gs = new GroupSymbol(table.getFullName());
        }

        resource.setTable(table);
        resource.setGroupSymbol(gs);
        resource.setEdmStructuredType(type);
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

        List<Column> pk = getPKColumns(resource.getTable());
        if (resource.getKeyPredicates().size() == 1) {
            if (pk.size() != 1) {
                throw new TeiidException(ODataPlugin.Event.TEIID16015,
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, resource.getTable().getFullName()));
            }
            Column column = pk.get(0);

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
        if (pk.size() != resource.getKeyPredicates().size()) {
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
            EdmPrimitiveTypeKind primitiveTypeKind = ODataTypeManager.odataType(column);
            expr = new LiteralImpl(key.getText(), odata.createPrimitiveTypeInstance(primitiveTypeKind));
        }
        return expr;
    }

    static Column findColumn(Table table, String propertyName) {
        return table.getColumnByName(propertyName);
    }

    public DocumentNode() {
    }

    public DocumentNode(Table table, GroupSymbol gs, EdmEntityType type) {
        this.table = table;
        this.groupSymbol = gs;
        this.edmStructuredType = type;
    }

    private Table getTable() {
        return table;
    }

    public String getName() {
        return table.getName();
    }

    public ContextColumn getColumnByName(String name) {
        Column c = this.table.getColumnByName(name);
        if (c != null) {
            return new TableContextColumn(c);
        }
        return null;
    }

    public String getFullName() {
        return table.getFullName();
    }

    public GroupSymbol getGroupSymbol() {
        return groupSymbol;
    }

    public EdmStructuredType getEdmStructuredType() {
        return edmStructuredType;
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

    public void setEdmStructuredType(EdmStructuredType edmStructuredType) {
        this.edmStructuredType = edmStructuredType;
    }

    protected void addAllColumns(boolean onlyPK) {
        if (onlyPK) {
            List<Column> columns = getPKColumns(getTable());
            for (final Column column : columns) {
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
            Expression expr) {
        EdmPropertyImpl edmProperty = (EdmPropertyImpl) this.edmStructuredType.getProperty(columnName);
        ContextColumn c = getColumnByName(columnName);
        /* currently not needed, but if we need to associate the column's srid or if we can rely on
         * the value for a variable srid, then we need to produce ewkb instead
          if (c.getDatatype().getName().equalsIgnoreCase(DataTypeManager.DefaultDataTypes.GEOMETRY)) {
            String val = c.getProperty(BaseColumn.SPATIAL_SRID, false);
            if (val != null) {
                expr = new Function("ST_SETSRID", new Expression[] {expr, new Constant(val)}); //$NON-NLS-1$
            }
            expr = new Function("ST_ASEWKB", new Expression[] {expr}); //$NON-NLS-1$
            expr = new AliasSymbol(columnName, expr);
        }*/
        ProjectedColumn pc = addProjectedColumn(expr, edmProperty.getType(), edmProperty, edmProperty.isCollection());
        pc.setOrdinal(c.getOrdinal());
    }

    protected ProjectedColumn addProjectedColumn(final Expression expr, final EdmType type,
            EdmProperty property, final boolean collection) {
        ProjectedColumn pc = this.projectedColumns.get(expr);
        if (pc != null) {
            return pc;
        }
        pc = new ProjectedColumn(expr, (SingletonPrimitiveType) type, property, collection);
        pc.setOrdinal(Integer.MAX_VALUE);
        this.projectedColumns.put(expr, pc);
        if (property != null) {
            this.projectedColumnsByName.put(property.getName(), pc);
        }
        return pc;
    }

    OrderBy addDefaultOrderBy() {
        if (this.table == null) {
            return null;
        }
        OrderBy orderBy = new OrderBy();
        // provide implicit ordering for cursor logic
        KeyRecord record = ODataSchemaBuilder.getIdentifier(this.table);
        // provide implicit ordering for cursor logic
        for (Column column:record.getColumns()) {
            ElementSymbol expr = new ElementSymbol(column.getName(), this.groupSymbol);
            //we'll assume that null values won't be part of the unique key
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
        for (DocumentNode er:this.siblings) {
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
        if (this.edmStructuredType instanceof EdmEntityType) {
            return ((EdmEntityType)this.edmStructuredType).getKeyPredicateNames();
        }
        return Collections.emptyList();
    }

    public void setKeyPredicates(List<UriParameter> keyPredicates) {
        this.keyPredicates = keyPredicates;
    }

    public void addSibling(DocumentNode resource) {
        this.siblings.add(resource);
    }

    public List<DocumentNode> getSiblings(){
        return this.siblings;
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
        addColumns(select, ordinal, sortColumns(getProjectedColumns().values()));
        for (DocumentNode sibiling:this.siblings) {
            addColumns(select, ordinal, sortColumns(sibiling.getProjectedColumns().values()));
        }

        Query query = new Query();
        From from = new From();

        from.addClause(this.fromClause);
        for (DocumentNode sibiling:this.siblings) {
            from.addClause(sibiling.getFromClause());
        }
        if (this.iterator != null) {
            addColumns(select, ordinal, sortColumns(this.iterator.getProjectedColumns().values()));
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

    protected List<ProjectedColumn> sortColumns(Collection<ProjectedColumn> toSort) {
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

    protected void addColumns(Select select, AtomicInteger ordinal,
            List<ProjectedColumn> projected) {
        for (ProjectedColumn column:projected) {
            select.addSymbol(column.getExpression());
            column.setOrdinal(ordinal.getAndIncrement());
        }
    }

    Criteria buildJoinCriteria(DocumentNode joinResource, EdmNavigationProperty property) throws TeiidException {
        KeyInfo keyInfo = joinFK(joinResource.getTable(), getTable(), property);
        if (keyInfo == null) {
            keyInfo = joinFK(getTable(), joinResource.getTable(), property);
            if (keyInfo == null) {
                throw new TeiidException("Fk not found");
            }
        }

        return buildCriteria(keyInfo.reverse?joinResource:this, keyInfo.reverse?this:joinResource, keyInfo.fk);
    }

    DocumentNode joinTable(DocumentNode joinResource, EdmNavigationProperty property, JoinType joinType) throws TeiidException {
        Criteria crit = null;
        if (!joinType.equals(JoinType.JOIN_CROSS)) {
            crit = buildJoinCriteria(joinResource, property);
        }

        FromClause fromClause;
        if (joinResource.getKeyPredicates() != null && joinResource.getKeyPredicates().size() > 0) {
            // here the previous entityset is verbose; need to be canonicalized
            fromClause = new UnaryFromClause(joinResource.getGroupSymbol());
        }
        else {
            fromClause = new JoinPredicate(this.getFromClause(), new UnaryFromClause(joinResource.getGroupSymbol()), joinType, crit);
        }

        joinResource.setFromClause(fromClause);
        return joinResource;
    }

    static ForeignKey joinFK(DocumentNode current, DocumentNode reference, EdmNavigationProperty property) {
        Table currentTable = current.getTable();
        Table referenceTable = reference.getTable();
        if (currentTable ==  null || referenceTable == null) {
            return null;
        }

        KeyInfo keyInfo = joinFK(currentTable, referenceTable, property);
        if (keyInfo != null) {
            return keyInfo.fk;
        }
        return null;
    }

    private static class KeyInfo {
        boolean reverse;
        ForeignKey fk;

        public KeyInfo(boolean reverse, ForeignKey fk) {
            this.reverse = reverse;
            this.fk = fk;
        }
    }

    private static KeyInfo joinFK(Table currentTable, Table referenceTable, EdmNavigationProperty property) {
        for (ForeignKey fk : currentTable.getForeignKeys()) {
            if (!referenceTable.getParent().equals(fk.getReferenceKey().getParent().getParent())
                    || !referenceTable.getName().equals(fk.getReferenceTableName())) {
                continue;
            }
            if (!property.isCollection() && property.getName().equals(fk.getName())) {
                return new KeyInfo(false, fk);
            }
            if (property.getName().equals(currentTable.getName() + "_" + fk.getName())) { //$NON-NLS-1$
                return new KeyInfo(true, fk);
            }
        }
        return null;
    }

    static Criteria buildJoinCriteria(DocumentNode from, DocumentNode to) {
        for (ForeignKey fk:from.getTable().getForeignKeys()) {
            if (fk.getReferenceKey().getParent().equals(to.getTable())) {
                return buildCriteria(from, to, fk);
            }
        }
        return null;
    }

    private static Criteria buildCriteria(DocumentNode from, DocumentNode to,
            ForeignKey fk) {
        List<String> fkColumns = DocumentNode.getColumnNames(fk.getColumns());
        if (fkColumns == null) {
            fkColumns = DocumentNode.getColumnNames(getPKColumns(from.getTable()));
        }

        List<String> pkColumns = DocumentNode.getColumnNames(fk.getReferenceKey().getColumns());
        Criteria criteria = DocumentNode.buildJoinCriteria(
                from.getGroupSymbol(),
                to.getGroupSymbol(), pkColumns, fkColumns);
        return criteria;
    }

    static List<Column> getPKColumns(Table table){
        return ODataSchemaBuilder.getIdentifier(table).getColumns();
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
