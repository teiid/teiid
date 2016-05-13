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
package org.teiid.odata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.odata4j.core.NamedValue;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OFunctionParameter;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.core.OSimpleObject;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmFunctionParameter;
import org.odata4j.edm.EdmProperty;
import org.odata4j.exceptions.NotAcceptableException;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.expression.AggregateAllFunction;
import org.odata4j.expression.AggregateAnyFunction;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.OrderByExpression;
import org.odata4j.expression.OrderByExpression.Direction;
import org.odata4j.producer.QueryInfo;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.util.Assertion;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.odata.DocumentNode.ProjectedColumn;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.translator.odata.ODataTypeManager;

public class ODataSQLBuilder extends ODataExpressionVisitor {
	private Query query = new Query();
	private OrderBy orderBy = new OrderBy();
	private Criteria where;
	private MetadataStore metadata;
	protected DocumentNode resultNode;
	private FromClause fromCluse = null;
	private HashMap<String, DocumentNode> assosiatedTables = new HashMap<String, DocumentNode>();
	private AtomicInteger groupCount = new AtomicInteger(0);
	private boolean distinct = false;

	public ODataSQLBuilder(MetadataStore metadata, boolean prepared) {
	    super(prepared);
		this.metadata = metadata;
	}
	
    public Query selectString(String entityName, QueryInfo info,
            OEntityKey key, String navProperty, boolean countStar) {
		Select select = new Select();

		LinkedHashSet<String> $select = new LinkedHashSet<String>();		
		if (info.select != null) {
			for (EntitySimpleProperty property:info.select) {
			    $select.add(property.getPropertyName());
			}
		}
		
		this.resultNode = buildDocumentNode(entityName);
		this.assosiatedTables.put(this.resultNode.getEntityTable().getFullName(), this.resultNode);
		
		if (key != null) {
			this.where = buildEntityKeyCriteria(this.resultNode, key);
		}
		
		this.fromCluse = new UnaryFromClause(this.resultNode.getEntityGroup());
		
		if (navProperty != null) {
			String prop = null;
			
			if (navProperty.startsWith("/")) {
				navProperty = navProperty.substring(1);
			}
			
			for (String segment:navProperty.split("/")) {
		        String[] propSplit = segment.split("\\(");
		        prop = propSplit[0];

		        Column column = findColumn(this.resultNode.getEntityTable(), prop);
		        if (column != null) {
		            if (!$select.isEmpty()) {
		                throw new NotAcceptableException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16020));
		            }
		            $select.add(column.getName());
		        	continue;
		        }
		        
		        // find association table.
	        	DocumentNode joinNode = joinTable(this.resultNode, buildDocumentNode(prop));
	            this.resultNode = joinNode;
	        	
	            if (propSplit.length > 1) {
	                key = OEntityKey.parse("("+ propSplit[1]);
                    this.where = Criteria.combineCriteria(
                            this.where,
                            buildEntityKeyCriteria(this.resultNode, key));
	            }
			}
		}
		
		if (info != null && info.expand != null && !info.expand.isEmpty()) {
		    if (info.expand.size() > 1) {
		        throw new UnsupportedOperationException("Only one $expand is suported"); //$NON-NLS-1$
		    }
            DocumentNode expandNode = joinTable(this.resultNode,
                    buildDocumentNode(info.expand.get(0).getPropertyName()));
		    this.resultNode.setExpandNode(expandNode);
		}
		
		if (countStar) {
            AggregateSymbol aggregateSymbol = new AggregateSymbol(
                    AggregateSymbol.Type.COUNT.name(), false, null);
			select = new Select(Arrays.asList(aggregateSymbol));
		}
		else {
            select = buildSelectColumns($select, this.resultNode);		
		}
		
		if (info.filter != null) {
			Assertion.assertTrue(this.isEmpty());
			visitNode(info.filter);
			Expression ex = this.getExpression();
			if (!(ex instanceof Criteria)) {
				ex = new ExpressionCriteria(ex);
			}
			this.where = Criteria.combineCriteria(this.where, (Criteria) ex);
		}

		if (!countStar) {
			// order by
			if (info.orderBy != null && !info.orderBy.isEmpty()) {
				for (OrderByExpression expr:info.orderBy) {
					visitNode(expr);
				}
				query.setOrderBy(this.orderBy);
			}
			else {
				KeyRecord record = this.resultNode.getEntityTable().getPrimaryKey();
				if (record == null) {
					// if PK is not available there MUST at least one unique key
					record = this.resultNode.getEntityTable().getUniqueKeys().get(0);
				}
				// provide implicit ordering for cursor logic
				for (Column column:record.getColumns()) {
                    OrderByExpression expr = org.odata4j.expression.Expression
                            .orderBy(org.odata4j.expression.Expression.simpleProperty(column.getName()),
                                    Direction.ASCENDING);
					visitNode(expr);
				}
				query.setOrderBy(this.orderBy);
			}
	
			select.setDistinct(this.distinct);
		}
		From from = new From();
		from.addClause(this.fromCluse);
		query.setSelect(select);
		query.setFrom(from);
		query.setCriteria(this.where);
		return query;
	}

    private DocumentNode buildDocumentNode(String name) {
        String alias = "g"+ this.groupCount.getAndIncrement();
        return buildDocumentNode(name, alias);
    }
    
    private DocumentNode buildDocumentNode(String name, String alias) {
        Table table = findTable(name, metadata);
        if (table == null) {
            throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16004, name));
        }
        return new DocumentNode(table, new GroupSymbol(alias, table.getFullName()));
    }
    

    private DocumentNode joinTable(DocumentNode parentNode, DocumentNode joinNode) {
        boolean associationFound = false;
        
        for (ForeignKey fk:joinNode.getEntityTable().getForeignKeys()) {
        	if (fk.getReferenceKey().getParent().equals(parentNode.getEntityTable())) {
        		if(this.assosiatedTables.get(joinNode.getEntityTable().getFullName()) == null) {
        			List<String> refColumns = fk.getReferenceColumns();
        			if (refColumns == null) {
        				refColumns = getColumnNames(parentNode.getEntityTable().getPrimaryKey().getColumns());
        			}	  
                    addJoinTable(joinNode, JoinType.JOIN_INNER, 
                            parentNode, refColumns,
                            getColumnNames(fk.getColumns()));	        				
        		}
        		associationFound = true;
        		break;
        	}
        }
        
        // if association not found; see at the other end of the reference
        for (ForeignKey fk:parentNode.getEntityTable().getForeignKeys()) {
            if (fk.getReferenceKey().getParent().equals(joinNode.getEntityTable())) {
                if(this.assosiatedTables.get(joinNode.getEntityTable().getFullName()) == null) {
                    List<String> refColumns = fk.getReferenceColumns();
                    if (refColumns == null) {
                        refColumns = getColumnNames(joinNode.getEntityTable().getPrimaryKey().getColumns());
                    }                   
                    addJoinTable(joinNode, JoinType.JOIN_INNER, 
                            parentNode, getColumnNames(fk.getColumns()),
                            refColumns);
                }
                associationFound = true;
                break;
            }
        }
        
        if (!associationFound) {
            throw new NotFoundException(ODataPlugin.Util.gs(
                    ODataPlugin.Event.TEIID16003, joinNode.getEntityTable().getFullName(),
                    parentNode.getEntityTable().getFullName()));
        }
        return joinNode;
    }
	
    public Query callFunctionQuery(String procedureName,
            EdmFunctionImport function, Map<String, OFunctionParameter> inputParams, QueryInfo info) {
        
        GroupSymbol gs = new GroupSymbol("g0");
        Procedure proc = findProcedure(procedureName, this.metadata);
        
        this.resultNode = new DocumentNode(proc, gs);
        
        // handle $select
        Select select = new Select();
        int ordinal = 1;
        if (info.select != null && !info.select.isEmpty()) {
            for (EntitySimpleProperty property:info.select) {
                for (Column column : proc.getResultSet().getColumns()) {
                    if (column.getName().equals(property.getPropertyName())){
                        this.resultNode.addProjectColumn(column.getName(), ordinal++, true, 
                                ODataTypeManager.odataType(column.getRuntimeType()));
                        select.addSymbol(new ElementSymbol(column.getName(), gs));
                    }
                }
                
            }
        } else {
            // empty projected column; means all
            select.addSymbol(new MultipleElementSymbol(gs.getName()));
        }

        //handle $orderby
        if (info.orderBy != null && !info.orderBy.isEmpty()) {
            for (OrderByExpression expr:info.orderBy) {
                visitNode(expr);
            }
            query.setOrderBy(this.orderBy);
        }

        // create a inline view from procedure
        StoredProcedure procedure = storedProcedure(procedureName, function, inputParams);
        SubqueryFromClause sfc = new SubqueryFromClause(gs, procedure);
        sfc.setLateral(true);        
        
        // handle $filter
        if (info.filter != null) {
            Assertion.assertTrue(this.isEmpty());
            visitNode(info.filter);
            Expression ex = this.getExpression();
            if (!(ex instanceof Criteria)) {
                ex = new ExpressionCriteria(ex);
            }
            this.where = Criteria.combineCriteria(this.where, (Criteria) ex);
        }        
        
        From from = new From();
        from.addClause(sfc);
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(this.where);
        return query;        
	}
	
    public StoredProcedure storedProcedure(String name,
            EdmFunctionImport function,
            Map<String, OFunctionParameter> inputParams) {
        StoredProcedure procedure = new StoredProcedure();
        procedure.setProcedureName(name);
        
        if (!inputParams.isEmpty()) {
            for (EdmFunctionParameter edmFunctionParameter : function.getParameters()) {
                OFunctionParameter param = inputParams.get(edmFunctionParameter.getName());
                if (param == null) {
                    continue;
                }                
                SPParameter spParam = new SPParameter(getParameters().size()+1, SPParameter.IN, 
                        edmFunctionParameter.getName());
                spParam.setExpression(new Reference(getParameters().size()));
                procedure.setParameter(spParam);
                
                Object value = ((OSimpleObject<?>)(param.getValue())).getValue();
                Integer sqlType = JDBCSQLTypeInfo.getSQLType(
                        ODataTypeManager.teiidType(param.getType().getFullyQualifiedTypeName()));
                getParameters().add(new SQLParam(ODataTypeManager.convertToTeiidRuntimeType(value), sqlType));
            }
        }
        procedure.setDisplayNamedParameters(true);
        return procedure;
    }
    
	private void addJoinTable(final DocumentNode joinNode, final JoinType joinType,
			final DocumentNode parentNode, List<String> pkColumns,
			List<String> refColumns) {
		
		List<Criteria> critList = new ArrayList<Criteria>();

		for (int i = 0; i < refColumns.size(); i++) {
            critList.add(new CompareCriteria(new ElementSymbol(
                    pkColumns.get(i), parentNode.getEntityGroup()), CompareCriteria.EQ,
                    new ElementSymbol(refColumns.get(i), joinNode.getEntityGroup())));
		}         			
		
		Criteria crit = critList.get(0);
		for (int i = 1; i < critList.size(); i++) {
			crit = new CompoundCriteria(CompoundCriteria.AND, crit, critList.get(i));
		}		        			
		
		if (this.fromCluse == null) {
            this.fromCluse = new JoinPredicate(
                    new UnaryFromClause(parentNode.getEntityGroup()), new UnaryFromClause(
                            joinNode.getEntityGroup()), JoinType.JOIN_INNER, crit);
		}
		else {
            this.fromCluse = new JoinPredicate(this.fromCluse,
                    new UnaryFromClause(joinNode.getEntityGroup()), joinType,
                    crit);
		}
		this.assosiatedTables.put(joinNode.getEntityTable().getFullName(), joinNode);
	}

	private Criteria buildEntityKeyCriteria(DocumentNode entityNode, OEntityKey entityKey) {
		KeyRecord pk = entityNode.getEntityTable().getPrimaryKey();
		
		if (entityKey.getKeyType() == OEntityKey.KeyType.SINGLE) {
			if (pk.getColumns().size() != 1) {
                throw new NotFoundException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID16015, entityNode.getEntityTable().getFullName(),
                        entityKey));
			}	
			Column column = entityNode.getEntityTable().getPrimaryKey().getColumns().get(0);
            return new CompareCriteria(new ElementSymbol(column.getName(),
                    entityNode.getEntityGroup()), CompareCriteria.EQ, new Constant(
                    entityKey.asSingleValue()));
		}

		// complex (multi-keyed)
		List<Criteria> critList = new ArrayList<Criteria>();
		Set<NamedValue<?>> keys = entityKey.asComplexValue();
		if (pk.getColumns().size() != keys.size()) {
            throw new NotFoundException(ODataPlugin.Util.gs(
                    ODataPlugin.Event.TEIID16015, entityNode.getEntityTable().getFullName(),
                    entityKey));
		}
		for (NamedValue<?> key : keys) {
			Column column = findColumn(entityNode.getEntityTable(), key.getName());
            critList.add(new CompareCriteria(new ElementSymbol(
                    column.getName(), entityNode.getEntityGroup()), CompareCriteria.EQ,
                    new Constant(key.getValue())));
		}
	
		return new CompoundCriteria(CompoundCriteria.AND, critList);
	}

    private Select buildSelectColumns(Set<String> selectColumns,
            DocumentNode parentNode) {
        Select select = new Select();
        int ordinal = 1;
		if (!selectColumns.isEmpty()) {
			// also add pk, fks for building the link keys
			for (Column column:parentNode.getEntityTable().getPrimaryKey().getColumns()) {
			    if (!parentNode.getProjectedColumns().containsKey(column.getName())) {
                    parentNode.addProjectColumn(column.getName(), ordinal++,
                            selectColumns.contains(column.getName()), 
                            ODataTypeManager.odataType(column.getRuntimeType()));
			    }
			}
			
			for (ForeignKey fk:parentNode.getEntityTable().getForeignKeys()) {
				for (Column column:fk.getColumns()) {				    
	                if (!parentNode.getProjectedColumns().containsKey(column.getName())) {
	                    parentNode.addProjectColumn(column.getName(), ordinal++,
	                            selectColumns.contains(column.getName()), 
	                            ODataTypeManager.odataType(column.getRuntimeType()));
	                }
				}
			}
			
			// project all these.
			for (String property:selectColumns) {
                Column column = findColumn(parentNode.getEntityTable(), property);
                if (column == null) {
                    throw new NotFoundException(ODataPlugin.Util.gs(
                            ODataPlugin.Event.TEIID16005, property,
                            parentNode.getEntityTable().getFullName()));
                }
                if (!parentNode.getProjectedColumns().containsKey(column.getName())) {
                    parentNode.addProjectColumn(column.getName(), ordinal++,
                            selectColumns.contains(column.getName()), 
                            ODataTypeManager.odataType(column.getRuntimeType()));
                }
			}
			
			for (ProjectedColumn pc : parentNode.getProjectedColumns().values()) {
			    select.addSymbol(new ElementSymbol(pc.name(), parentNode.getEntityGroup()));
			}

		} else {
		    //empty projected columns, means all 
		    
			//use select x.* to avoid selecting unselectable columns
			//and possibly to exclude columns that the user is not entitled to
			select.addSymbol(new MultipleElementSymbol(parentNode.getEntityGroup().getName()));			
		}
		
        if (parentNode.getExpandNode() != null) {
          //empty projected columns, means all 
            select.addSymbol(new MultipleElementSymbol(parentNode.getExpandNode().getEntityGroup().getName()));
        }
		return select;
	}

	@Override
	public void visit(String type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(OrderByExpression expr) {
		visitNode(expr.getExpression());
		visit(expr.getDirection());
	}

	@Override
	public void visit(Direction direction) {
		Expression expr = getExpression();
		this.orderBy.addVariable(expr, direction == Direction.ASCENDING);
	}

	@Override
	public void visit(EntitySimpleProperty expr) {
		String property = expr.getPropertyName();
		
		if (property.indexOf('/') == -1) {
		    setExpression(new ElementSymbol(property, this.resultNode.getEntityGroup()));
			return;
		}
		
		// this is to support the filter clause with URI like
		// http://host/service.svc/Orders?$filter = Customer/ContactName ne 'Fred'
		// we need to handle 'Customer/ContactName'
		// only supporting one level deep for simplicity
		String[] segments = property.split("/");
		DocumentNode joinNode = joinTable(this.resultNode, buildDocumentNode(segments[0]));
		Column column = findColumn(joinNode.getEntityTable(), segments[1]);
		if (column == null) {
            throw new NotFoundException(ODataPlugin.Util.gs(
                    ODataPlugin.Event.TEIID16005, segments[1],
                    joinNode.getEntityTable().getFullName()));
		}
		setExpression(new ElementSymbol(segments[1], joinNode.getEntityGroup()));
	}

	@Override
    public GroupSymbol getDocumentGroup() {
        return this.resultNode.getEntityGroup();
    }

	@Override
	public void visit(AggregateAnyFunction expr) {
		// http://host/service.svc/Orders?$filter=OrderLines/any(ol: ol/Quantity gt 10)
		// select distinct orders.* from orders join orderlines on (key) where (orderlines.quantity > 10)
		String joinTableName = ((EntitySimpleProperty)expr.getSource()).getPropertyName();
		final DocumentNode joinNode = buildDocumentNode(joinTableName, expr.getVariable());
		joinTable(this.resultNode, joinNode);
		//expr.getPredicate().visitThis(this);
		ODataExpressionVisitor ev = new ODataExpressionVisitor(isPrepared()) {
		    @Override
		    public GroupSymbol getDocumentGroup() {
		        return joinNode.getEntityGroup();
		    }
		};
		expr.getPredicate().visitThis(ev);
		setExpression(ev.getExpression());
		this.distinct = true;
	}

	@Override
	public void visit(AggregateAllFunction expr) {
		// http://host/service.svc/Orders?$filter=OrderLines/all(ol: ol/Quantity gt 10)
		// select * from orders where ALL (select quantity from orderlines where fk = pk) > 10
		String tblName = ((EntitySimpleProperty)expr.getSource()).getPropertyName();
		Table joinTable = findTable(tblName, this.metadata);
		if (joinTable == null) {
			throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16009, tblName));
		}
		DocumentNode joinNode = buildDocumentNode(tblName, expr.getVariable());
		ODataAggregateAnyBuilder builder = new ODataAggregateAnyBuilder(expr, this.resultNode, joinNode);
		Criteria criteria = builder.getCriteria();
		setExpression(criteria);
	}
	
	private Table findTable(EdmEntitySet entity, MetadataStore store) {
		return findTable(entity.getType().getFullyQualifiedTypeName(), store);
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
		Table result = null;
		for (Schema s : store.getSchemaList()) {
			Table t = s.getTables().get(tableName);
			if (t != null) {
				if (result != null) {
					throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16017, tableName));
				}
				result = t;
			}
		}		
		return result;
	}

    private Procedure findProcedure(String procedureName, MetadataStore store) {
        int idx = procedureName.indexOf('.');
        if (idx > 0) {
            Schema s = store.getSchema(procedureName.substring(0, idx));
            if (s != null) {
                Procedure t = s.getProcedure(procedureName.substring(idx+1));
                if (t != null) {
                    return t;
                }
            }
        }
        Procedure result = null;
        for (Schema s : store.getSchemaList()) {
            Procedure t = s.getProcedures().get(procedureName);
            if (t != null) {
                if (result != null) {
                    throw new NotFoundException(ODataPlugin.Util.gs(
                            ODataPlugin.Event.TEIID16017, procedureName));
                }
                result = t;
            }
        }       
        return result;
    }	
	private Column findColumn(Table table, String propertyName) {
		return table.getColumnByName(propertyName);
	}

	public DocumentNode getDocumentNode() {
		return this.resultNode;
	}	
	
	OrderBy getOrderBy(){
		return this.orderBy;
	}
	
	public Insert insert(EdmEntitySet entitySet, OEntity entity) {

		Table entityTable = findTable(entitySet, this.metadata);
		this.resultNode = new DocumentNode(entityTable, new GroupSymbol(entityTable.getFullName()));
	    
		List values = new ArrayList();
		
		Insert insert = new Insert();
		insert.setGroup(this.resultNode.getEntityGroup());
		
		int i = 0;
	    for (OProperty<?> prop : entity.getProperties()) {
	      EdmProperty edmProp = entitySet.getType().findProperty(prop.getName());
	      Column column = entityTable.getColumnByName(edmProp.getName());
	      insert.addVariable(new ElementSymbol(column.getName(), this.resultNode.getEntityGroup()));
	        
	      values.add(new Reference(i++));
	      getParameters().add(asParam(prop, edmProp));
	    }
	    
	    insert.setValues(values);
	    return insert;
	}
	
	//TODO: allow the generated key building.
    public OEntityKey buildEntityKey(EdmEntitySet entitySet, OEntity entity,
            Map<String, Object> generatedKeys) {
		Table entityTable = findTable(entitySet, this.metadata);
		KeyRecord pk = entityTable.getPrimaryKey();
		List<OProperty<?>> props = new ArrayList<OProperty<?>>();
		for (Column c:pk.getColumns()) {
			OProperty<?> prop = null;
			try {
				prop = entity.getProperty(c.getName());
			} catch (Exception e) {
				Object value = generatedKeys.get(c.getName());
				if (value == null) {
                    throw new NotFoundException(ODataPlugin.Util.gs(
                            ODataPlugin.Event.TEIID16016,
                            entity.getEntitySetName()));
				}
				prop = OProperties.simple(c.getName(),value);
			}
			props.add(prop);
		}
		return OEntityKey.infer(entitySet, props);
	}

	public Delete delete(EdmEntitySet entitySet, OEntityKey entityKey) {
		
		Table entityTable = findTable(entitySet, this.metadata);
		this.resultNode = new DocumentNode(entityTable, new GroupSymbol(entityTable.getFullName()));

		Delete delete = new Delete();
		delete.setGroup(this.resultNode.getEntityGroup());
		delete.setCriteria(buildEntityKeyCriteria(this.resultNode, entityKey));
		
		return delete;
	}

	public Update update(EdmEntitySet entitySet, OEntity entity) {
		Table entityTable = findTable(entitySet, this.metadata);
		this.resultNode = new DocumentNode(entityTable, new GroupSymbol(entityTable.getFullName()));
		
		Update update = new Update();
		update.setGroup(this.resultNode.getEntityGroup());
		update.setCriteria(buildEntityKeyCriteria(this.resultNode, entity.getEntityKey()));
		
		int i = 0;
		for (OProperty<?> prop : entity.getProperties()) {
			EdmProperty edmProp = entitySet.getType().findProperty(prop.getName());
			Column column = entityTable.getColumnByName(edmProp.getName());
			boolean add = true;
			for (Column c:entityTable.getPrimaryKey().getColumns()) {
				if (c.getName().equals(column.getName())) {
					add = false;
				}
			}
			if (add) {
                update.addChange(new ElementSymbol(column.getName(),
                        this.resultNode.getEntityGroup()), new Reference(i++));
				getParameters().add(asParam(prop, edmProp));
			}
		}
		return update;
	}

	private SQLParam asParam(OProperty<?> prop, EdmProperty edmProp) {
        return new SQLParam(ODataTypeManager.convertToTeiidRuntimeType(prop
                .getValue()), JDBCSQLTypeInfo.getSQLType(ODataTypeManager
                .teiidType(edmProp.getType().getFullyQualifiedTypeName())));
	}
	
	static List<String> getColumnNames(List<Column> columns){
		ArrayList<String> columnNames = new ArrayList<String>();
		for (Column column:columns) {
			columnNames.add(column.getName());
		}
		return columnNames;
	}
}
