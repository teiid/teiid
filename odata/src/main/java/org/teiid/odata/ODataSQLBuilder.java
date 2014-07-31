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

import static org.teiid.language.SQLConstants.Reserved.*;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.odata4j.core.NamedValue;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmProperty;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.expression.*;
import org.odata4j.expression.OrderByExpression.Direction;
import org.odata4j.producer.QueryInfo;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.odata.ODataTypeManager;

public class ODataSQLBuilder extends ODataHierarchyVisitor {
	private Query query = new Query();
	private OrderBy orderBy = new OrderBy();
	private Criteria criteria;
	private MetadataStore metadata;
	private ArrayList<SQLParam> params = new ArrayList<SQLParam>();
	private boolean prepared = true;
	private Stack<Expression> stack = new Stack<Expression>();
	private GroupSymbol resultEntityGroup;
	private Table resultEntityTable; // this is the original entity table for results
	private FromClause fromCluse = null;
	private HashMap<String, GroupSymbol> assosiatedTables = new HashMap<String, GroupSymbol>();
	private LinkedHashMap<String, Boolean> projectedColumns = new LinkedHashMap<String, Boolean>();
	private HashMap<String, String> aliasTableNames = new HashMap<String, String>();
	private AtomicInteger groupCount = new AtomicInteger(1);
	private boolean distinct = false;
	private boolean useLimit = true;

	public ODataSQLBuilder(MetadataStore metadata, boolean prepared) {
		this.metadata = metadata;
		this.prepared = prepared;
	}
	
	public void setUseLimit(boolean useLimit) {
		this.useLimit = useLimit;
	}

	public Query selectString(String entityName, QueryInfo info, OEntityKey key, String navProperty, boolean countStar) {
		Select select = new Select();

		if (info.select != null) {
			for (EntitySimpleProperty property:info.select) {
				this.projectedColumns.put(property.getPropertyName(), Boolean.TRUE);
			}
		}
		
		Table entityTable = findTable(entityName, metadata);
		this.resultEntityTable = entityTable;
		this.resultEntityGroup = new GroupSymbol("g0", entityName);
		this.assosiatedTables.put(entityTable.getName(), this.resultEntityGroup);
		this.aliasTableNames.put("g0", entityTable.getName());
		
		if (key != null) {
			this.criteria = buildEntityKeyCriteria(entityTable, this.resultEntityGroup, key);
		}
		
		this.fromCluse = new UnaryFromClause(this.resultEntityGroup);
		
		if (navProperty != null) {
			String prop = null;
			
			if (navProperty.startsWith("/")) {
				navProperty = navProperty.substring(1);
			}
			
			for (String segment:navProperty.split("/")) {
		        String[] propSplit = segment.split("\\(");
		        prop = propSplit[0];

		        Column column = findColumn(entityTable, prop);
		        if (column != null) {
		        	this.projectedColumns.clear();
		        	this.projectedColumns.put(column.getName(), Boolean.TRUE);
		        	continue;
		        }
		        
		        // find association table.
	        	Table joinTable = findTable(prop, metadata);
	        	if (joinTable == null) {
	        		throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16004, prop));
	        	}
	        	
	        	boolean associationFound = false;
	        	String aliasGroup = "g"+this.groupCount.getAndIncrement();
	        	
	        	for (ForeignKey fk:joinTable.getForeignKeys()) {
	        		if (fk.getReferenceKey().getParent().equals(entityTable)) {

	        			if(this.assosiatedTables.get(joinTable.getName()) == null) {
		        			List<String> refColumns = fk.getReferenceColumns();
		        			if (refColumns == null) {
		        				refColumns = getColumnNames(entityTable.getPrimaryKey().getColumns());
		        			}	  
		        			addJoinTable(aliasGroup, JoinType.JOIN_INNER, joinTable, entityTable, refColumns, getColumnNames(fk.getColumns()));	        				
	        			}
	        			associationFound = true;
	        			break;
	        		}
	        	}
	        	
	        	// if association not found; see at the other end of the reference
	        	if (!associationFound) {
	            	for (ForeignKey fk:entityTable.getForeignKeys()) {
	            		if (fk.getReferenceKey().getParent().equals(joinTable)) {
	            			if(this.assosiatedTables.get(joinTable.getName()) == null) {
	            				List<String> refColumns = fk.getReferenceColumns();
	            				if (refColumns == null) {
	            					refColumns = getColumnNames(joinTable.getPrimaryKey().getColumns());
	            				}    				
	            				addJoinTable(aliasGroup, JoinType.JOIN_INNER, joinTable, entityTable, getColumnNames(fk.getColumns()), refColumns);    				
	            			}
		        			associationFound = true;
		        			break;	            			
	            		}
	            	}	        		
	        	}
	        	
	        	if (!associationFound) {
	        		throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16003, prop, resultEntityTable.getName()));
	        	}
    			
    			entityTable = joinTable;
    			this.resultEntityGroup = this.assosiatedTables.get(joinTable.getName());
    			this.resultEntityTable = entityTable;	        	
	        	
	            if (propSplit.length > 1) {
	                key = OEntityKey.parse("("+ propSplit[1]);
	                if (this.criteria != null) {
	                	this.criteria = new CompoundCriteria(CompoundCriteria.AND, this.criteria, buildEntityKeyCriteria(entityTable, this.resultEntityGroup, key));
	                }
	                else {
	                	this.criteria = buildEntityKeyCriteria(entityTable, this.resultEntityGroup, key);
	                }
	            }
			}
		}
		
		if (countStar) {
			AggregateSymbol aggregateSymbol = new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null);
			select = new Select(Arrays.asList(aggregateSymbol));
		}
		else {
			select = buildSelectColumns(this.projectedColumns, entityTable, this.resultEntityGroup);		
		}
		
		if (info.filter != null) {
			visitNode(info.filter);
		}

		if (!countStar) {
			// order by
			List<OrderByExpression> orderBy = info.orderBy;
			if (orderBy != null && !orderBy.isEmpty()) {
				for (OrderByExpression expr:info.orderBy) {
					visitNode(expr);
				}
				query.setOrderBy(this.orderBy);
			}
			else {
				KeyRecord record = resultEntityTable.getPrimaryKey();
				if (record == null) {
					// if PK is not available there MUST at least one unique key
					record = resultEntityTable.getUniqueKeys().get(0);
				}
				// provide implicit ordering for cursor logic
				for (Column column:record.getColumns()) {
					OrderByExpression expr = org.odata4j.expression.Expression.orderBy(org.odata4j.expression.Expression.simpleProperty(column.getName()), Direction.ASCENDING);
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
		query.setCriteria(this.criteria);
		return query;
	}
	
	private GroupSymbol joinTable(String tableName, final String alias, final JoinType joinType) {
		Table joinTable = findTable(tableName, metadata);
    	if (joinTable == null ) {
    		tableName = this.aliasTableNames.get(tableName);
    		joinTable = findTable(tableName, metadata);
    	}
    	if (joinTable == null && alias != null) {
    		tableName = this.aliasTableNames.get(alias);
    		joinTable = findTable(tableName, metadata);
    	}
    	if (joinTable == null) {
    		throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16004, tableName));
    	}
    	
    	String joinKey = (alias != null)?alias:joinTable.getName();
    	String aliasGroup = (alias == null)?"g"+this.groupCount.getAndIncrement():alias;
    	
    	for (ForeignKey fk:this.resultEntityTable.getForeignKeys()) {
    		if (fk.getReferenceKey().getParent().equals(joinTable)) {
    			if(this.assosiatedTables.get(joinKey) == null) {
    				List<String> refColumns = fk.getReferenceColumns();
    				if (refColumns == null) {
    					refColumns = getColumnNames(joinTable.getPrimaryKey().getColumns());
    				}    				
    				addJoinTable(aliasGroup, joinType, joinTable, this.resultEntityTable, getColumnNames(fk.getColumns()), refColumns);    				
    			}
    			return this.assosiatedTables.get(alias!=null?aliasGroup:joinTable.getName());
    		}
    	}
    	
    	// if join direction is other way
    	for (ForeignKey fk:joinTable.getForeignKeys()) {
    		if (fk.getReferenceKey().getParent().equals(this.resultEntityTable)) {
    			if(this.assosiatedTables.get(joinKey) == null) {
    				List<String> refColumns = fk.getReferenceColumns();
    				if (refColumns == null) {
    					refColumns = getColumnNames(this.resultEntityTable.getPrimaryKey().getColumns());
    				}    				
    				addJoinTable(aliasGroup, joinType, joinTable, this.resultEntityTable, refColumns, getColumnNames(fk.getColumns()));
    			}
    			return this.assosiatedTables.get(alias!=null?aliasGroup:joinTable.getName());
    		}
    	}
    	return null;
	}

	private void addJoinTable(final String alias, final JoinType joinType,
			final Table joinTable, final Table entityTable, List<String> pkColumns,
			List<String> refColumns) {
		
		GroupSymbol joinGroup = new GroupSymbol(alias, joinTable.getName());
		GroupSymbol entityGroup = this.assosiatedTables.get(entityTable.getName());
		
		List<Criteria> critList = new ArrayList<Criteria>();

		for (int i = 0; i < refColumns.size(); i++) {
			critList.add(new CompareCriteria(new ElementSymbol(pkColumns.get(i), entityGroup), CompareCriteria.EQ, new ElementSymbol(refColumns.get(i), joinGroup)));
		}         			
		
		Criteria crit = critList.get(0);
		for (int i = 1; i < critList.size(); i++) {
			crit = new CompoundCriteria(CompoundCriteria.AND, crit, critList.get(i));
		}		        			
		
		if (this.fromCluse == null) {
			this.fromCluse = new JoinPredicate(new UnaryFromClause(entityGroup), new UnaryFromClause(joinGroup), JoinType.JOIN_INNER, crit);
			
		}
		else {
			this.fromCluse = new JoinPredicate(this.fromCluse, new UnaryFromClause(joinGroup), joinType, crit);
		}
		this.assosiatedTables.put(alias, joinGroup);
		this.assosiatedTables.put(joinTable.getName(), joinGroup);
		this.aliasTableNames.put(alias, joinTable.getName());
	}

	private Criteria buildEntityKeyCriteria(Table table, GroupSymbol entityGroup, OEntityKey entityKey) {
		KeyRecord pk = table.getPrimaryKey();
		
		if (entityKey.getKeyType() == OEntityKey.KeyType.SINGLE) {
			if (pk.getColumns().size() != 1) {
				throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, table.getFullName(), entityKey));
			}	
			Column column = table.getPrimaryKey().getColumns().get(0);
			return new CompareCriteria(new ElementSymbol(column.getName(), entityGroup), CompareCriteria.EQ, new Constant(entityKey.asSingleValue()));
		}

		// complex (multi-keyed)
		List<Criteria> critList = new ArrayList<Criteria>();
		Set<NamedValue<?>> keys = entityKey.asComplexValue();
		if (pk.getColumns().size() != keys.size()) {
			throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16015, table.getFullName(), entityKey));
		}
		for (NamedValue<?> key : keys) {
			Column column = findColumn(table, key.getName());
			critList.add(new CompareCriteria(new ElementSymbol(column.getName(), entityGroup), CompareCriteria.EQ, new Constant(key.getValue())));
		}
	
		return new CompoundCriteria(CompoundCriteria.AND, critList);
	}

	private Select buildSelectColumns(HashMap<String, Boolean> selectColumns, Table table, GroupSymbol group) {
		Select select = new Select();
		if (!selectColumns.isEmpty()) {
			// also add pk, fks for building the link keys
			for (Column column:table.getPrimaryKey().getColumns()) {
				selectColumns.put(column.getName(), (selectColumns.get(column.getName()) != null));
			}
			
			for (ForeignKey fk:table.getForeignKeys()) {
				for (Column column:fk.getColumns()) {
					selectColumns.put(column.getName(), (selectColumns.get(column.getName()) != null));
				}
			}
			
			// project all these.
			for (String property:selectColumns.keySet()) {
				Column column = findColumn(table, property);
				if (column == null) {
					throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16005, property, table.getFullName()));
				}
				select.addSymbol(new ElementSymbol(column.getName(), group));
			}
		} else {
			for (Column c:table.getColumns()) {
				select.addSymbol(new ElementSymbol(c.getName(), group));
				selectColumns.put(c.getName(), Boolean.TRUE);
			}
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
		Expression expr = stack.pop();
		if (expr instanceof CompareCriteria) {
			expr = ((CompareCriteria)expr).getLeftExpression();
		}
		this.orderBy.addVariable(expr, direction == Direction.ASCENDING);
	}

	@Override
	public void visit(AddExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();
		stack.push(new Function("+", new Expression[] {lhs, rhs})); //$NON-NLS-1$
	}

	@Override
	public void visit(AndExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();
		this.criteria = new CompoundCriteria(CompoundCriteria.AND, (Criteria)lhs, (Criteria)rhs);
		stack.push(this.criteria);
	}

	@Override
	public void visit(BooleanLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.BOOLEAN));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(CastExpression expr) {
		visitNode(expr.getExpression());
		Expression rhs = new Constant(ODataTypeManager.teiidType(expr.getType()));
		Expression lhs = stack.pop();
		stack.push(new Function(CONVERT, new Expression[] {lhs, rhs})); 
	}

	@Override
	public void visit(ConcatMethodCallExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		stack.push(new Function("CONCAT2", new Expression[] {lhs, rhs})); //$NON-NLS-1$	
	}

	@Override
	public void visit(DateTimeLiteral expr) {
		Timestamp timestamp = new Timestamp(expr.getValue().toDateTime().getMillis());
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(timestamp, Types.TIMESTAMP));
		}
		else {
			stack.add(new Constant(timestamp)); 
		}
	}

	@Override
	public void visit(DateTimeOffsetLiteral expr) {
		throw new UnsupportedOperationException();

	}

	@Override
	public void visit(DecimalLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.DECIMAL));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(DivExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		stack.push(new Function("/", new Expression[] {lhs, rhs})); //$NON-NLS-1$			
	}

	@Override
	public void visit(EndsWithMethodCallExpression expr) {
		visitNode(expr.getTarget());
		Expression target = stack.pop();
		visitNode(expr.getValue());
		Expression value = stack.pop();		
		this.criteria = new CompareCriteria(new Function("ENDSWITH", new Expression[] {target, value}), CompareCriteria.EQ, new Constant(Boolean.TRUE));
		stack.push(this.criteria);
	}

	@Override
	public void visit(EntitySimpleProperty expr) {
		String property = expr.getPropertyName();
		
		if (property.indexOf('/') == -1) {
			stack.push(new ElementSymbol(property, this.resultEntityGroup));
			return;
		}
		
		// this is to support the filter clause with URI like
		// http://host/service.svc/Orders?$filter = Customer/ContactName ne 'Fred'
		// we need to handle 'Customer/ContactName'
		// only supporting one level deep for simplicity
		String[] segments = property.split("/"); 
		GroupSymbol joinGroup = joinTable(segments[0], null, JoinType.JOIN_INNER);
		Table joinTable = findTable(joinGroup.getDefinition(), metadata);
		Column column = findColumn(joinTable, segments[1]);
		if (column == null) {
			throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16005, segments[1], joinTable.getFullName()));
		}
		this.stack.push(new ElementSymbol(segments[1], joinGroup));
	}

	@Override
	public void visit(EqExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();
		if (rhs instanceof Constant && ((Constant)rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
			this.criteria = new IsNullCriteria(lhs);
		}
		else {
			this.criteria = new CompareCriteria(lhs, CompareCriteria.EQ, rhs);
		}
		stack.push(this.criteria);
	}

	@Override
	public void visit(GeExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		this.criteria = new CompareCriteria(lhs, CompareCriteria.GE, rhs);
		stack.push(this.criteria);
	}

	@Override
	public void visit(GtExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		this.criteria = new CompareCriteria(lhs, CompareCriteria.GT, rhs);
		stack.push(this.criteria);
	}

	@Override
	public void visit(GuidLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue().toString(), Types.VARCHAR));
		}
		else {
			stack.add(new Constant(expr.getValue().toString())); 
		}
	}

	@Override
	public void visit(BinaryLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.BINARY));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(ByteLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.TINYINT));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(SByteLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.TINYINT));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(IndexOfMethodCallExpression expr) {
		visitNode(expr.getValue());
		visitNode(expr.getTarget());
		Expression target = stack.pop();
		Expression value = stack.pop();		
		stack.push(new Function("LOCATE", new Expression[] {value, target})); //$NON-NLS-1$
	}

	@Override
	public void visit(SingleLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.FLOAT));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(DoubleLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.DOUBLE));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(IntegralLiteral expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.INTEGER));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(Int64Literal expr) {
		if (this.prepared) {
			stack.add(new Reference(this.params.size())); 
			this.params.add(new SQLParam(expr.getValue(), Types.BIGINT));
		}
		else {
			stack.add(new Constant(expr.getValue())); 
		}
	}

	@Override
	public void visit(IsofExpression expr) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(LeExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		this.criteria = new CompareCriteria(lhs, CompareCriteria.LE, rhs);
		stack.push(this.criteria);
	}

	@Override
	public void visit(LengthMethodCallExpression expr) {
		visitNode(expr.getTarget());	
		stack.push(new Function("LENGTH", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(LtExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		this.criteria = new CompareCriteria(lhs, CompareCriteria.LT, rhs);
		stack.push(this.criteria);
	}

	@Override
	public void visit(ModExpression expr) {
		visitNode(expr.getLHS());	
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		stack.push(new Function("MOD", new Expression[] {lhs, rhs})); //$NON-NLS-1$
	}

	@Override
	public void visit(MulExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();		
		stack.push(new Function("*", new Expression[] {lhs, rhs})); //$NON-NLS-1$
	}

	@Override
	public void visit(NeExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();
		if (rhs instanceof Constant && ((Constant)rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
			IsNullCriteria crit = new IsNullCriteria(lhs);
			crit.setNegated(true);
			this.criteria = crit;
		}
		else {
			this.criteria = new CompareCriteria(lhs, CompareCriteria.NE, rhs);
		}
		stack.push(this.criteria);
	}

	@Override
	public void visit(NegateExpression expr) {
		visitNode(expr.getExpression());
		Expression ex = stack.pop();
		stack.push(new Function(SourceSystemFunctions.MULTIPLY_OP, new Expression[] {new Constant(-1), ex})); 
	}

	@Override
	public void visit(NotExpression expr) {
		visitNode(expr.getExpression());
		this.criteria = new NotCriteria(new ExpressionCriteria(stack.pop()));
		stack.push(this.criteria);
	}

	@Override
	public void visit(NullLiteral expr) {
		stack.push(new Constant(null));
	}

	@Override
	public void visit(OrExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();
		this.criteria = new CompoundCriteria(CompoundCriteria.OR, (Criteria)lhs, (Criteria)rhs);
		stack.push(this.criteria);
	}

	@Override
	public void visit(ParenExpression expr) {
		visitNode(expr.getExpression());
	}

	@Override
	public void visit(BoolParenExpression expr) {
		visitNode(expr.getExpression());
	}

	@Override
	public void visit(ReplaceMethodCallExpression expr) {
		List<Expression> expressions = new ArrayList<Expression>();
		visitNode(expr.getTarget());
		expressions.add(stack.pop());
		visitNode(expr.getFind());
		expressions.add(stack.pop());
		visitNode(expr.getReplace());
		expressions.add(stack.pop());
		stack.push(new Function("REPLACE", expressions.toArray(new Expression[expressions.size()]))); //$NON-NLS-1$
	}

	@Override
	public void visit(StartsWithMethodCallExpression expr) {
		visitNode(expr.getTarget());
		Expression target = stack.pop();
		visitNode(expr.getValue());
		Expression value = stack.pop();		
		this.criteria = new CompareCriteria(new Function("LOCATE", new Expression[] {value, target, new Constant(1)}), CompareCriteria.EQ, new Constant(1));
		stack.push(this.criteria);
	}

	@Override
	public void visit(StringLiteral expr) {
		if (this.prepared) {
			stack.push(new Reference(this.params.size()));
			this.params.add(new SQLParam(expr.getValue(), Types.VARCHAR));
		}
		else {
			stack.push(new Constant(expr.getValue()));
		}
	}

	@Override
	public void visit(SubExpression expr) {
		visitNode(expr.getLHS());
		visitNode(expr.getRHS());
		Expression rhs = stack.pop();
		Expression lhs = stack.pop();
		stack.push(new Function("-", new Expression[] {lhs, rhs})); //$NON-NLS-1$		
	}

	@Override
	public void visit(SubstringMethodCallExpression expr) {
		visitNode(expr.getTarget());
		List<Expression> expressions = new ArrayList<Expression>();
		expressions.add(stack.pop());
		if (expr.getStart() != null) {
			visitNode(expr.getStart());
			expressions.add(stack.pop());
		}
		if (expr.getLength() != null) {
			visitNode(expr.getLength());
			expressions.add(stack.pop());
		}
		stack.push(new Function("SUBSTRING", expressions.toArray(new Expression[expressions.size()]))); //$NON-NLS-1$		
	}

	@Override
	public void visit(SubstringOfMethodCallExpression expr) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(TimeLiteral expr) {
		Time time = new Time(expr.getValue().toDateTimeToday().getMillis());
		if (this.prepared) {
			stack.push(new Reference(this.params.size()));
			this.params.add(new SQLParam(time, Types.TIME));
		}
		else {
			stack.push(new Constant(time));
		}		
	}

	@Override
	public void visit(ToLowerMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("LCASE", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(ToUpperMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("UCASE", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(TrimMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("TRIM", new Expression[] {new Constant("BOTH"), new Constant(' '), stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(YearMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("YEAR", new Expression[] {stack.pop()})); //$NON-NLS-1$		
	}

	@Override
	public void visit(MonthMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("MONTH", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(DayMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("DAYOFMONTH", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(HourMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("HOUR", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(MinuteMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("MINUTE", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(SecondMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("SECOND", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(RoundMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("ROUND", new Expression[] {stack.pop(), new Constant(0)})); //$NON-NLS-1$
	}

	@Override
	public void visit(FloorMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("FLOOR", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(CeilingMethodCallExpression expr) {
		visitNode(expr.getTarget());
		stack.push(new Function("CEILING", new Expression[] {stack.pop()})); //$NON-NLS-1$
	}

	@Override
	public void visit(AggregateAnyFunction expr) {
		// http://host/service.svc/Orders?$filter=OrderLines/any(ol: ol/Quantity gt 10)
		// select distinct orders.* from orders join orderlines on (key) where (orderlines.quantity > 10)
		String joinTableName = ((EntitySimpleProperty)expr.getSource()).getPropertyName();
		joinTable(joinTableName, expr.getVariable(), JoinType.JOIN_INNER);
		expr.getPredicate().visitThis(this);
		this.distinct = true;
	}

	@Override
	public void visit(AggregateAllFunction expr) {
		//throw new UnsupportedOperationException("TODO");
		// http://host/service.svc/Orders?$filter=OrderLines/all(ol: ol/Quantity gt 10)
		// select * from orders where ALL (select quantity from orderlines where fk = pk) > 10
		
		String tblName = ((EntitySimpleProperty)expr.getSource()).getPropertyName();
		Table joinTable = findTable(tblName, this.metadata);
		GroupSymbol joinGroup = new GroupSymbol(expr.getVariable(),tblName);
		
		if (joinTable == null) {
			throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16009, tblName));
		}
		
		ODataAggregateAnyBuilder builder = new ODataAggregateAnyBuilder(expr,
				this.resultEntityTable, this.resultEntityGroup, joinTable, joinGroup);
		this.criteria = builder.getCriteria();
		stack.push(criteria);
	}

	private Table findTable(String tableName, MetadataStore store) {
		for (Schema s : store.getSchemaList()) {
			for (Table t : s.getTables().values()) {
				if (t.getFullName().equals(tableName)) {
					return t;
				}
			}
		}
		for (Schema s : store.getSchemaList()) {
			for (Table t : s.getTables().values()) {
				if (t.getName().equals(tableName)) {
					return t;
				}
			}
		}		
		return null;
	}

	private Column findColumn(Table table, String propertyName) {
		return table.getColumnByName(propertyName);
	}

	public List<SQLParam> getParameters(){
		return this.params;
	}
	
	public Table getEntityTable() {
		return this.resultEntityTable;
	}	
	
	Expression getExpression() {
		return stack.pop();
	}
	
	OrderBy getOrderBy(){
		return this.orderBy;
	}
	
	LinkedHashMap<String, Boolean> getProjectedColumns(){
		return this.projectedColumns;
	}
	
	public Insert insert(EdmEntitySet entitySet, OEntity entity) {

		Table entityTable = findTable(entitySet.getName(), this.metadata);
		this.resultEntityTable = entityTable;
		this.resultEntityGroup = new GroupSymbol(entitySet.getName());
	    
		List values = new ArrayList();
		
		Insert insert = new Insert();
		insert.setGroup(this.resultEntityGroup);
		
		int i = 0;
	    for (OProperty<?> prop : entity.getProperties()) {
	      EdmProperty edmProp = entitySet.getType().findProperty(prop.getName());
	      Column column = entityTable.getColumnByName(edmProp.getName());
	      insert.addVariable(new ElementSymbol(column.getName(), this.resultEntityGroup));
	        
	      values.add(new Reference(i++));
	      this.params.add(asParam(prop, edmProp));
	    }
	    
	    insert.setValues(values);
	    return insert;
	}
	
	//TODO: allow the generated key building.
	public OEntityKey buildEntityKey(EdmEntitySet entitySet, OEntity entity, Map<String, Object> generatedKeys) {
		Table entityTable = findTable(entitySet.getName(), this.metadata);
		KeyRecord pk = entityTable.getPrimaryKey();
		List<OProperty<?>> props = new ArrayList<OProperty<?>>();
		for (Column c:pk.getColumns()) {
			OProperty prop = null;
			try {
				prop = entity.getProperty(c.getName());
			} catch (Exception e) {
				Object value = generatedKeys.get(c.getName());
				if (value == null) {
					// I observed with mysql did not return the label for column, 
					// this may be workaround in single key case in compound case 
					// we got to error.
					if (pk.getColumns().size() == 1 && generatedKeys.size() == 1) {
						value = generatedKeys.values().iterator().next();
					}					
					if (value == null) {
						throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16016, entity.getEntitySetName()));
					}
				}
				prop = OProperties.simple(c.getName(),value);
			}
			props.add(prop);
		}
		return OEntityKey.infer(entitySet, props);
	}

	public Delete delete(EdmEntitySet entitySet, OEntityKey entityKey) {
		
		Table entityTable = findTable(entitySet.getName(), this.metadata);
		this.resultEntityTable = entityTable;
		this.resultEntityGroup = new GroupSymbol(entitySet.getName());

		Delete delete = new Delete();
		delete.setGroup(this.resultEntityGroup);
		delete.setCriteria(buildEntityKeyCriteria(entityTable, this.resultEntityGroup, entityKey));
		
		return delete;
	}

	public Update update(EdmEntitySet entitySet, OEntity entity) {
		Table entityTable = findTable(entitySet.getName(), this.metadata);
		this.resultEntityTable = entityTable;
		this.resultEntityGroup = new GroupSymbol(entitySet.getName());
		
		Update update = new Update();
		update.setGroup(this.resultEntityGroup);
		update.setCriteria(buildEntityKeyCriteria(entityTable, this.resultEntityGroup, entity.getEntityKey()));
		
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
				update.addChange(new ElementSymbol(column.getName(),this.resultEntityGroup), new Reference(i++));
				this.params.add(asParam(prop, edmProp));
			}
		}
		return update;
	}

	private SQLParam asParam(OProperty<?> prop, EdmProperty edmProp) {
		return new SQLParam(ODataTypeManager.convertToTeiidRuntimeType(prop.getValue()), JDBCSQLTypeInfo.getSQLType(ODataTypeManager.teiidType(edmProp.getType().getFullyQualifiedTypeName())));
	}
	
	static List<String> getColumnNames(List<Column> columns){
		ArrayList<String> columnNames = new ArrayList<String>();
		for (Column column:columns) {
			columnNames.add(column.getName());
		}
		return columnNames;
	}
}
