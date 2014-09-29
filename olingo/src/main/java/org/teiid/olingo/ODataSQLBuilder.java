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
package org.teiid.olingo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.teiid.core.TeiidException;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;

public class ODataSQLBuilder extends DefaultODataResourceURLHierarchyVisitor implements ODataQueryContext {
	private MetadataStore metadata;
	private boolean prepared = true;
	private Map<String, ProjectedColumn> projectedColumns = new HashMap<String, ProjectedColumn>();
	private ArrayList<SQLParam> params = new ArrayList<SQLParam>();
	private AtomicInteger groupCount = new AtomicInteger(1);
	
	private FromClause fromClause;
	private Criteria criteria;
	private Select select = new Select();
	private ArrayList<TeiidException> exceptions = new ArrayList<TeiidException>();
	private EdmEntitySet edmEntitySet;
	private Table edmEntityTable;
	private GroupSymbol edmEntityTableGroup;
	private SkipOption skipOption;
	private TopOption topOption;
	private boolean countOption;
	private OrderBy orderBy;
		
	public ODataSQLBuilder(MetadataStore metadata, boolean prepared) {
		this.metadata = metadata;
		this.prepared = prepared;
	}
	
	public EdmEntitySet getEntitySet() {
		return this.edmEntitySet;
	}
	
	@Override
	public Table getEdmEntityTable() {
		return this.edmEntityTable;
	}

	@Override
	public GroupSymbol getEdmEntityTableGroup() {
		return this.edmEntityTableGroup;
	}	
	
	public boolean isCountQuery() {
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
	
	public Query selectQuery(boolean countQuery) throws TeiidException {
		if (!this.exceptions.isEmpty()) {
			throw this.exceptions.get(0);
		}
		
		Query query = new Query();
		From from = new From();
		from.addClause(this.fromClause);
		query.setSelect(this.select);
		query.setFrom(from);
		query.setCriteria(this.criteria);
		
		if (countQuery) {
			AggregateSymbol aggregateSymbol = new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null);
			this.select = new Select(Arrays.asList(aggregateSymbol));
			query.setSelect(this.select);
		}
		else {
			if (this.topOption != null && this.skipOption != null) {
				query.setLimit(new Limit(new Constant(this.skipOption.getValue()), new Constant(this.topOption.getValue())));
			}
			else if (this.topOption != null) {
				query.setLimit(new Limit(new Constant(0), new Constant(this.topOption.getValue())));
			}
		}
		
		if (this.orderBy != null & !countQuery) {
			query.setOrderBy(this.orderBy);
		}
		return query;		
	}
	
	Collection<ProjectedColumn> getProjectedColumns(){
		return this.projectedColumns.values();
	}
	
	private Table findTable(EdmEntitySet entitySet, MetadataStore store) {
		return findTable(entitySet.getEntityType(), store);
	}
	
	private Table findTable(EdmEntityType entityType, MetadataStore store) {
		Schema schema = store.getSchema(entityType.getNamespace());
		return schema.getTable(entityType.getName());
	}

	private Column findColumn(Table table, String propertyName) {
		return table.getColumnByName(propertyName);
	}
	
	public List<SQLParam> getParameters(){
		return this.params;
	}
	
	@Override
	public void visit(UriResourceEntitySet info) {
		this.edmEntitySet = info.getEntitySet();
		this.edmEntityTable = findTable(edmEntitySet, this.metadata);
		this.edmEntityTableGroup = new GroupSymbol("g0", this.edmEntityTable.getFullName()); //$NON-NLS-1$
		this.fromClause = new UnaryFromClause(this.edmEntityTableGroup);
		
		// URL is like /entitySet(key)s
		if (info.getKeyPredicates() != null && !info.getKeyPredicates().isEmpty()) {
			List<UriParameter> keys = info.getKeyPredicates();
			try {
				this.criteria = buildEntityKeyCriteria(this.edmEntityTable, this.edmEntityTableGroup, keys);
			} catch (TeiidException e) {
				this.exceptions.add(e);
			}
		}
	}
	
	private Criteria buildEntityKeyCriteria(Table table, GroupSymbol entityGroup, List<UriParameter> keys) throws TeiidException {
		KeyRecord pk = table.getPrimaryKey();
		
		if (keys.size() == 1) {
			if (pk.getColumns().size() != 1) {
				throw new TeiidException(ODataPlugin.Event.TEIID16015, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, table.getFullName()));
			}	
			Column column = table.getPrimaryKey().getColumns().get(0);
			ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(this, false, getUriInfo());
			return new CompareCriteria(new ElementSymbol(column.getName(), entityGroup), CompareCriteria.EQ, visitor.getExpression(keys.get(0).getExpression()));
		}

		// complex (multi-keyed)
		List<Criteria> critList = new ArrayList<Criteria>();
		if (pk.getColumns().size() != keys.size()) {
			throw new TeiidException(ODataPlugin.Event.TEIID16015, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, table.getFullName()));
		}
		for (UriParameter key : keys) {
			Column column = findColumn(table, key.getName());
			ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(this, false, getUriInfo());
			critList.add(new CompareCriteria(new ElementSymbol(column.getName(), entityGroup), CompareCriteria.EQ, visitor.getExpression(key.getExpression())));
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
		this.select = new Select();
		if (option == null) {
			// default select columns
			addAllColumns();			
		}
		else {
			for (SelectItem si:option.getSelectItems()) {
				if (si.isStar()) {
					addAllColumns();
					continue;
				}
				
				UriResource resource = ResourcePropertyCollector.getUriResource(si.getResourcePath());
				if (resource.getKind() != UriResourceKind.primitiveProperty) {
					this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16025, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16025)));
					continue;
				}
				UriResourcePrimitiveProperty primitiveProp = (UriResourcePrimitiveProperty)resource;
				addSelectColumn(primitiveProp.getProperty().getName());
			}
		}
	}

	private void addAllColumns() {
		for (final Column column : this.edmEntityTable.getColumns()) {
			addSelectColumn(column.getName());
		}
	}

	private void addSelectColumn(final String columnName) {
		this.select.addSymbol(new ElementSymbol(columnName, this.edmEntityTableGroup));
		addProjectedColumn(columnName, true);
	}

	private void addProjectedColumn(final String columnName, final boolean visibility) {
		this.projectedColumns.put(columnName, new ProjectedColumn() {
			@Override
			public String getName() {
				return columnName;
			}
			@Override
			public boolean isVisible() {
				return visibility;
			}
		});
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
				this.orderBy.addVariable(new ElementSymbol(column.getName(), this.edmEntityTableGroup));
				ProjectedColumn pc = this.projectedColumns.get(column.getName());
				if (pc == null) {
					addProjectedColumn(column.getName(), false);
				}
			}
		}
		else {
			List<OrderByItem> orderBys = option.getOrders();
			for (OrderByItem orderby:orderBys) {
				ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(this, false, getUriInfo());
				Expression expr = visitor.getExpression(orderby.getExpression());
				this.orderBy.addVariable(expr, !orderby.isDescending());
				if (expr instanceof ElementSymbol) {
					ProjectedColumn pc = this.projectedColumns.get(((ElementSymbol)expr).getName());
					if (pc == null) {
						addProjectedColumn(((ElementSymbol)expr).getName(), false);
					}					
				}
			}
		}
	}
	
	@Override
	public void visit(FilterOption info) {
		ODataExpressionToSQLVisitor visitor = new ODataExpressionToSQLVisitor(this, this.prepared, getUriInfo());
		this.criteria = (Criteria)visitor.getExpression(info.getExpression());
	}
	
	@Override
	public void visit(UriResourceNavigation info) {
		// typically navigation only happens in $entity-id situations,
		EdmNavigationProperty property = info.getProperty();
		String navigationName = property.getName();
		EdmEntityType type = property.getType();
		    	
    	String aliasGroup = getNextAliasGroup();
    	
    	for (ForeignKey fk : this.edmEntityTable.getForeignKeys()) {
    		if (fk.getName().equals(navigationName)) {
    			List<String> refColumns = fk.getReferenceColumns();
    			if (refColumns == null) {
    				refColumns = getColumnNames(this.edmEntityTable.getPrimaryKey().getColumns());
    			}
    			Table joinTable = findTable(type, this.metadata);
    			GroupSymbol joinGroup = new GroupSymbol(aliasGroup, joinTable.getFullName());
    	    			
    	    	List<UriParameter> keys = info.getKeyPredicates();
    	    	try {
					if (keys != null && keys.size() > 0) {
						// here the previous entityset is verbose; need to be canonicalized
					   	this.criteria = buildEntityKeyCriteria(joinTable, joinGroup, keys);
					   	this.fromClause = new UnaryFromClause(joinGroup); 
					}
					else {    	    		
						this.fromClause = addJoinTable(JoinType.JOIN_INNER, joinGroup, this.edmEntityTableGroup, refColumns, getColumnNames(fk.getColumns()));
					}
	    			this.edmEntityTableGroup = joinGroup;
	    			this.edmEntityTable = joinTable;
				} catch (TeiidException e) {
					this.exceptions.add(e);
				}
    			break;
    		}    		
    	}    	
	}

	@Override
	public String getNextAliasGroup() {
		String aliasGroup = "g"+this.groupCount.getAndIncrement(); //$NON-NLS-1$
		return aliasGroup;
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
}
