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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.language.*;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MutableDBRef.Assosiation;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MongoDBSelectVisitor extends HierarchyVisitor {
	private static final String EMBEDIN = "EMBEDIN"; //$NON-NLS-1$
	private static final String EMBEDDABLE = "EMBEDDABLE"; //$NON-NLS-1$

    private AtomicInteger aliasCount = new AtomicInteger();
	protected MongoDBExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	private Select command;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

	protected Stack<DBObject> onGoingCriteria = new Stack<DBObject>();
	protected Stack<Object> onGoingExpression  = new Stack<Object>();
	protected ConcurrentHashMap<Object, ColumnAlias> expressionMap = new ConcurrentHashMap<Object, ColumnAlias>();
	private HashMap<String, Object> groupByProjections = new HashMap<String, Object>();
	protected ColumnAlias onGoingAlias;

	protected MutableDBRef pushKey;
	protected List<MutableDBRef> pullKeys = new ArrayList<MutableDBRef>();
	protected LinkedHashMap<List<String>, MutableDBRef> foreignKeys = new LinkedHashMap<List<String>, MutableDBRef>();
	protected ArrayList<MutableDBRef> tableCopiedIn = new ArrayList<MutableDBRef>();

	// derived stuff
	protected Table collectionTable;
	protected BasicDBObject project = new BasicDBObject();
	protected Integer limit;
	protected Integer skip;
	protected DBObject sort;
	protected DBObject match;
	protected DBObject having;
	protected DBObject group;
	protected ArrayList<String> selectColumns = new ArrayList<String>();
	protected ArrayList<String> selectColumnReferences = new ArrayList<String>();
	protected boolean projectBeforeMatch = false;
	protected LinkedList<String> unwindTables = new LinkedList<String>();
	protected TableReference joinParentTable = null;
	protected boolean processJoin = true;
	protected ArrayList<Condition> pendingCondictions = new ArrayList<Condition>();

	public MongoDBSelectVisitor(MongoDBExecutionFactory executionFactory, RuntimeMetadata metadata) {
		this.executionFactory = executionFactory;
		this.metadata = metadata;
	}

    /**
     * Appends the string form of the LanguageObject to the current buffer.
     * @param obj the language object instance
     */
    public void append(LanguageObject obj) {
        if (obj != null) {
            visitNode(obj);
        }
    }

    /**
     * Simple utility to append a list of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items a list of LanguageObjects
     */
    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }

    /**
     * Simple utility to append an array of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items an array of LanguageObjects
     */
    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                append(items[i]);
            }
        }
    }

	public static String getRecordName(AbstractMetadataRecord object) {
		String nameInSource = object.getNameInSource();
        if(nameInSource != null && nameInSource.length() > 0) {
            return nameInSource;
        }
        return object.getName();
    }

	public String getColumnName(ColumnReference obj) {
		String elemShortName = null;
		AbstractMetadataRecord elementID = obj.getMetadataObject();
        if(elementID != null) {
            elemShortName = getRecordName(elementID);
        } else {
            elemShortName = obj.getName();
        }
		return elemShortName;
	}

	@Override
	public void visit(DerivedColumn obj) {
		this.onGoingAlias = buildAlias(obj.getAlias());
		append(obj.getExpression());

		Object expr = this.onGoingExpression.pop();

		ColumnAlias previousAlias = this.expressionMap.putIfAbsent(expr, this.onGoingAlias);
		if (previousAlias == null) {
			previousAlias = this.onGoingAlias;
		}

		if (this.command != null && this.command.isDistinct()) {
			this.groupByProjections.put(previousAlias.projName, expr);
		}

		if (obj.getExpression() instanceof ColumnReference) {
			String elementName = getColumnName((ColumnReference)obj.getExpression());
			this.selectColumnReferences.add(elementName);
			// the the expression is already part of group by then the projection should be $_id.{name}
			Object id = this.groupByProjections.get("_id"); //$NON-NLS-1$
			if (id == null && this.groupByProjections.get(previousAlias.projName) != null) {
				// this is DISTINCT case
				this.project.append(this.onGoingAlias.projName, "$_id."+previousAlias.projName); //$NON-NLS-1$
				this.selectColumns.add(this.onGoingAlias.projName);
			}
			else if (id != null
					&& ((id instanceof String) && id.equals(previousAlias.projName))
					|| ((id instanceof BasicDBObject) && ((BasicDBObject) id).get(previousAlias.projName) != null)) {
				this.project.append(this.onGoingAlias.projName, "$_id."+previousAlias.projName); //$NON-NLS-1$
				this.selectColumns.add(this.onGoingAlias.projName);
			}
			else {
				this.project.append(this.onGoingAlias.projName, expr);
				this.selectColumns.add(this.onGoingAlias.projName);
			}
		}
		else {
			// what user sees as project
			this.selectColumns.add(previousAlias.projName);
			this.selectColumnReferences.add(previousAlias.projName);
		}
		this.onGoingAlias = null;
	}

	private ColumnAlias buildAlias(String alias) {
		if (alias == null) {
			String str = "_m"+this.aliasCount.getAndIncrement(); //$NON-NLS-1$
			return new ColumnAlias(str, str);
		}
		return new ColumnAlias(alias, alias);
	}

	@Override
	public void visit(ColumnReference obj) {
		String elemShortName = getColumnName(obj);

		if (obj.getMetadataObject() == null) {
			for (Object expr:this.expressionMap.keySet()) {
				ColumnAlias alias = this.expressionMap.get(expr);
				if (alias.projName.equals(elemShortName)) {
					this.onGoingExpression.push(expr);
					break;
				}
			}
		}
		else {
			String selectionName = elemShortName;
			Table columnParent = obj.getTable().getMetadataObject();
			String columnName = obj.getMetadataObject().getName();

			// column is on the same collection
			if (columnParent.getName().equals(this.collectionTable.getName())) {
				// check if this is primary key
				if (isPartOfPrimaryKey(obj.getTable(), columnName)) {
					if (hasCompositePrimaryKey(obj.getTable())) {
						elemShortName = "_id."+columnName; //$NON-NLS-1$
						selectionName = elemShortName;
					}
					else {
						elemShortName = "_id"; //$NON-NLS-1$
						selectionName = elemShortName;
					}
				}
				if (isPartOfForeignKey(columnParent, columnName)) {
					// if this column is foreign key on same table then access must be "key.$id" because it is DBRef
					selectionName = elemShortName + ".$id"; //$NON-NLS-1$
					if (isMultiKeyForeignKey(columnParent, columnName)) {
						selectionName = selectionName+"."+columnName; //$NON-NLS-1$
					}
				}
			}
			else {
				// if this is embddable/embedIn table, then we need to use the embedded collection name
				String embedIn = columnParent.getProperty(EMBEDIN, false);
				boolean embeddable = Boolean.parseBoolean(columnParent.getProperty(EMBEDDABLE, false));
				if (embedIn != null || embeddable){
					elemShortName = columnParent.getName()+"."+columnName; //$NON-NLS-1$

					if (isPartOfPrimaryKey(columnParent, columnName)) {
						if (hasCompositePrimaryKey(columnParent)) {
							elemShortName = columnParent.getName()+"."+"_id."+columnName; //$NON-NLS-1$ //$NON-NLS-2$
							selectionName = elemShortName;
						}
						else {
							elemShortName = columnParent.getName()+"."+"_id"; //$NON-NLS-1$ //$NON-NLS-2$
							selectionName = elemShortName;
						}
					}
					if (isPartOfForeignKey(columnParent, columnName)) {
						// if this column is foreign key on same table then access must be "key.$id" because it is DBRef
						selectionName = elemShortName + ".$id"; //$NON-NLS-1$
						if (isMultiKeyForeignKey(columnParent, columnName)) {
							selectionName = selectionName+"."+columnName; //$NON-NLS-1$
						}
					}
				}
			}
			String mongoExpr = "$"+elemShortName; //$NON-NLS-1$
			this.onGoingExpression.push(mongoExpr);

			if (this.onGoingAlias == null) {
				this.expressionMap.putIfAbsent(mongoExpr, new ColumnAlias(elemShortName, selectionName));
			}
		}
	}

    @Override
	public void visit(AggregateFunction obj) {
    	if (!obj.getParameters().isEmpty()) {
    		append(obj.getParameters());
    	}
    	else {
			// this is only true for count(*) case, so we need implicit group id clause
    		this.onGoingExpression.push(new Integer(1));
    		this.groupByProjections.put("_id", null); //$NON-NLS-1$
    	}

    	BasicDBObject expr = null;
		if (obj.getName().equals(AggregateFunction.COUNT)) {
			expr = new BasicDBObject("$sum", this.onGoingExpression.pop()); //$NON-NLS-1$
		}
		else if (obj.getName().equals(AggregateFunction.AVG)) {
			expr = new BasicDBObject("$avg", this.onGoingExpression.pop()); //$NON-NLS-1$
		}
		else if (obj.getName().equals(AggregateFunction.SUM)) {
			expr = new BasicDBObject("$sum", this.onGoingExpression.pop()); //$NON-NLS-1$
		}
		else if (obj.getName().equals(AggregateFunction.MIN)) {
			expr = new BasicDBObject("$min", this.onGoingExpression.pop()); //$NON-NLS-1$
		}
		else if (obj.getName().equals(AggregateFunction.MAX)) {
			expr = new BasicDBObject("$max", this.onGoingExpression.pop()); //$NON-NLS-1$
		}
		else {
			this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18005, obj.getName())));
		}

		if (expr != null) {
			ColumnAlias alias = addToProject(expr, false);
			if (!this.groupByProjections.values().contains(expr)) {
				this.groupByProjections.put(alias.projName, expr);
			}
			this.onGoingExpression.push(expr);
		}
    }

	private ColumnAlias addToProject(BasicDBObject expr, boolean addExprAsProject) {
		ColumnAlias previousAlias = this.expressionMap.get(expr);
		if (previousAlias == null) {
			// if expression is in having clause there is will be no alias; however mongo expects this
			// to be elevated to grouping clause
			previousAlias = this.onGoingAlias;
			if (this.onGoingAlias == null) {
				this.projectBeforeMatch = true;
				previousAlias = buildAlias(null);
			}
			this.expressionMap.putIfAbsent(expr, previousAlias);
		}

		if (this.project.get(previousAlias.projName) == null && !this.project.values().contains(expr)) {
			this.project.append(previousAlias.projName, addExprAsProject?expr:1);
		}
		return previousAlias;
	}

	@Override
	public void visit(Function obj) {
    	if (this.executionFactory.getFunctionModifiers().containsKey(obj.getName())) {
            this.executionFactory.getFunctionModifiers().get(obj.getName()).translate(obj);
    	}
    	BasicDBObject expr = null;
    	List<Expression> args = obj.getParameters();
		if (args != null) {
			BasicDBList params = new BasicDBList();
			for (int i = 0; i < args.size(); i++) {
				append(args.get(i));
				Object param = this.onGoingExpression.pop();
				params.add(param);
			}
			expr = new BasicDBObject(obj.getName(), params);
		}

		if(expr != null) {
			addToProject(expr, true);
			this.onGoingExpression.push(expr);
		}
	}

	@Override
	public void visit(NamedTable obj) {
		this.collectionTable = obj.getMetadataObject();
		try {
			buildForeignDocumentKeys(obj);
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}


	@Override
	public void visit(Join obj) {

		if (obj.getLeftItem() instanceof Join) {
			this.processJoin = false;
			append(obj.getLeftItem());
			processJoin(this.joinParentTable, obj.getRightItem(), obj.getCondition());
			this.processJoin = true;
		}
		else {
			processJoin(obj.getLeftItem(), obj.getRightItem(), obj.getCondition());
		}

		if (this.processJoin) {
			append(this.joinParentTable);
		}
	}

	private void processJoin(TableReference leftTable, TableReference rightTable, Condition cond) {
		Table left = ((NamedTable)leftTable).getMetadataObject();
		Table right = ((NamedTable)rightTable).getMetadataObject();

        String embedInTable = left.getProperty(EMBEDIN, false);
        boolean embeddable = Boolean.parseBoolean(left.getProperty(EMBEDDABLE, false));

        boolean singleDocument = false;

        // If the left table is "embedIn" then right is parent.
        if ((embedInTable != null && embedInTable.equals(right.getName())) || embeddable) {
        	this.joinParentTable = rightTable;
        	if (addUnwind(right, left)) {
        		this.unwindTables.add(left.getName());
        	}
        	singleDocument = true;
        }

        if (!singleDocument) {
	        embedInTable = right.getProperty(EMBEDIN, false);
	        embeddable = Boolean.parseBoolean(right.getProperty(EMBEDDABLE, false));

	        // If the right table is "embedIn" then left is parent.
	        if ((embedInTable != null && embedInTable.equals(left.getName())) || embeddable) {
	        	this.joinParentTable = leftTable;
	        	if (addUnwind(left, right)) {
	        		this.unwindTables.add(right.getName());
	        	}
	        	singleDocument = true;
	        }
        }

        if (!singleDocument) {
        	this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18012, left.getName(), right.getName())));
        }

        if (cond != null) {
        	this.pendingCondictions.add(cond);
        }
	}

    private boolean addUnwind(Table parent, Table child) {
    	// if parent table has FK to child then it is 1 to MANY, if reverse then MANY to 1
    	for (ForeignKey fk:parent.getForeignKeys()) {
    		if (fk.getReferenceTableName().equals(child.getName())) {
    			return false;
    		}
    	}

    	for (ForeignKey fk:child.getForeignKeys()) {
    		if (fk.getReferenceTableName().equals(parent.getName())) {
    			return true;
    		}
    	}
    	return false;
	}

	@Override
    public void visit(Select obj) {
    	this.command = obj;

        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
        	append(obj.getFrom());
        }

        if (!this.exceptions.isEmpty()) {
        	return;
        }

        if (obj.getWhere() != null) {
            append(obj.getWhere());
        }

        if (!this.onGoingCriteria.isEmpty()) {
        	if (this.match != null) {
        		this.match = QueryBuilder.start().and(this.match, this.onGoingCriteria.pop()).get();
        	}
        	else {
        		this.match = this.onGoingCriteria.pop();
        	}
        }

        if (obj.getGroupBy() != null) {
            append(obj.getGroupBy());
        }

    	append(obj.getDerivedColumns());

    	// if group by does not exist then build the group root id based on distinct
		if (obj.getGroupBy() == null && obj.isDistinct() && this.groupByProjections != null
				&& !this.groupByProjections.containsKey("_id")) { //$NON-NLS-1$
			// in distinct since there may not be group by, but mongo requires a grouping clause.
			BasicDBObject _id = new BasicDBObject(this.groupByProjections);
			this.groupByProjections.clear();
			this.groupByProjections.put("_id", _id); //$NON-NLS-1$
		}

        if (obj.getHaving() != null) {
            append(obj.getHaving());
        }

        if (!this.onGoingCriteria.isEmpty()) {
        	this.having = this.onGoingCriteria.pop();
        }

        if (!this.groupByProjections.isEmpty()) {
            if (!this.groupByProjections.containsKey("_id")) { //$NON-NLS-1$
            	this.groupByProjections.put("_id", null); //$NON-NLS-1$
            }
        	this.group = new BasicDBObject(this.groupByProjections);
        }

        if (obj.getOrderBy() != null) {
            append(obj.getOrderBy());
        }

        if (obj.getLimit() != null) {
        	 append(obj.getLimit());
        }
    }

	@Override
	public void visit(Comparison obj) {
        QueryBuilder query = getQueryObject(obj.getLeftExpression());
        append(obj.getRightExpression());

        Object rightExpr = this.onGoingExpression.pop();
        if (this.expressionMap.get(rightExpr) != null) {
        	rightExpr = this.expressionMap.get(rightExpr).projName;
        }
        if (query != null) {
        	switch(obj.getOperator()) {
	        case EQ:
	        	query.is(rightExpr);
	        	break;
	        case NE:
	        	query.notEquals(rightExpr);
	        	break;
	        case LT:
	        	query.lessThan(rightExpr);
	        	break;
	        case LE:
	        	query.lessThanEquals(rightExpr);
	        	break;
	        case GT:
	        	query.greaterThan(rightExpr);
	        	break;
	        case GE:
	        	query.greaterThanEquals(rightExpr);
	        	break;
	        }
        	this.onGoingCriteria.push(query.get());
        }
	}

	@Override
    public void visit(AndOr obj) {
		append(obj.getLeftCondition());
		append(obj.getRightCondition());
        DBObject right = this.onGoingCriteria.pop();
        DBObject left = this.onGoingCriteria.pop();

        switch(obj.getOperator()) {
        case AND:
        	this.onGoingCriteria.push(QueryBuilder.start().and(left, right).get());
        	break;
        case OR:
        	this.onGoingCriteria.push(QueryBuilder.start().or(left, right).get());
        	break;
        }
    }

	@Override
	public void visit(Literal obj) {
		try {
			this.onGoingExpression.push(this.executionFactory.convertToMongoType(obj.getValue(), null, null));
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}

	@Override
	public void visit(In obj) {
        QueryBuilder query = getQueryObject(obj.getLeftExpression());

    	append(obj.getRightExpressions());

        BasicDBList values = new BasicDBList();
        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
        	values.add(0, this.onGoingExpression.pop());
        }

		if (query != null) {
			if (obj.isNegated()) {
				query.notIn(values);
			} else {
				query.in(values);
			}
			this.onGoingCriteria.push(query.get());
		}
	}

	private QueryBuilder getQueryObject(Expression obj) {
		// the way DBRef names handled in projection vs selection is different.
		// in projection we want to see as "col" mapped to "col.$_id" as it is treated as sub-document
		// where as in selection it will should be "col._id".
		append(obj);

		Object expr = this.onGoingExpression.pop();
		ColumnAlias exprAlias = this.expressionMap.get(expr);
		if (exprAlias == null) {
			//exprAlias = buildAlias(null);
			//this.expressionMap.put(expr, exprAlias);
		}

		// when expression shows up in a condition, but it is not a derived column
		// then add implicit project on that alias.
		return QueryBuilder.start(exprAlias.selName);
	}

	@Override
	public void visit(IsNull obj) {
		QueryBuilder query = getQueryObject(obj.getExpression());
		if (query != null) {
			if (obj.isNegated()) {
				query.notEquals(null);
			}
			else {
				query.is(null);
			}
			this.onGoingCriteria.push(query.get());
		}
	}

	@Override
	public void visit(Like obj) {
		QueryBuilder query = getQueryObject(obj.getLeftExpression());
		if (query != null) {
			if (obj.isNegated()) {
				query.not();
			}

			append(obj.getRightExpression());

			StringBuilder value = new StringBuilder((String)this.onGoingExpression.pop());
			int idx = -1;
			while (true) {
				idx = value.indexOf("%", idx+1);//$NON-NLS-1$
				if (idx != -1 && idx == 0) {
					continue;
				}
				if (idx != -1 && idx == value.length()-1) {
					continue;
				}

				if (idx == -1) {
					break;
				}
				value.replace(idx, idx+1, ".*"); //$NON-NLS-1$
			}

			if (value.charAt(0) != '%') {
				value.insert(0, '^');
			}

			idx = value.length();
			if (value.charAt(idx-1) != '%') {
				value.insert(idx, '$');
			}

			String regex = value.toString().replaceAll("%", ""); //$NON-NLS-1$ //$NON-NLS-2$
			query.is("/"+regex+"/"); //$NON-NLS-1$ //$NON-NLS-2$
			//query.regex(Pattern.compile(regex));
			this.onGoingCriteria.push(query.get());
		}
	}

	@Override
	public void visit(Limit obj) {
		this.limit = new Integer(obj.getRowLimit());
		this.skip = new Integer(obj.getRowOffset());
	}

	@Override
	public void visit(OrderBy obj) {
		append(obj.getSortSpecifications());
	}

	@Override
	public void visit(SortSpecification obj) {
		append(obj.getExpression());
		Object expr = this.onGoingExpression.pop();
		ColumnAlias alias = this.expressionMap.get(expr);
		if (this.sort == null) {
			this.sort =  new BasicDBObject(alias.projName, (obj.getOrdering() == Ordering.ASC)?1:-1);
		}
		else {
			this.sort.put(alias.projName, (obj.getOrdering() == Ordering.ASC)?1:-1);
		}
	}

	@Override
	public void visit(GroupBy obj) {
		if (obj.getElements().size() == 1) {
			append(obj.getElements().get(0));
			Object mongoExpr = this.onGoingExpression.pop();
			this.groupByProjections.put("_id", mongoExpr); //$NON-NLS-1$
		}
		else {
			BasicDBObject fields = new BasicDBObject();
			for (Expression expr : obj.getElements()) {
				append(expr);
				Object mongoExpr = this.onGoingExpression.pop();
				ColumnAlias alias = this.expressionMap.get(mongoExpr);
				fields.put(alias.projName, mongoExpr);
			}
			this.groupByProjections.put("_id", fields); //$NON-NLS-1$

		}
	}

	protected void buildForeignDocumentKeys(NamedTable obj) throws TranslatorException {
		Table table = obj.getMetadataObject();
        String embedInTableName = table.getProperty(EMBEDIN, false);
        boolean embeddable = Boolean.parseBoolean(table.getProperty(EMBEDDABLE, false));

        if (embeddable && embedInTableName != null) {
        	this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18013, table.getName())));
        	return;
        }

        if (embeddable) {
        	// check to what other tables got relations to this table
        	for (Table t:table.getParent().getTables().values()) {
        		for (ForeignKey fk:t.getForeignKeys()) {
        			if (fk.getReferenceTableName().equals(table.getName())){
        				MutableDBRef ref = new MutableDBRef();
        				ref.setParentTable(t.getName());
        				ref.setEmbeddedTable(table.getName());
        				ref.setColumns(getColumnNames(fk.getColumns()));
        				ref.setReferenceColumns(fk.getReferenceColumns());
        				ref.setAssosiation(Assosiation.ONE);
        				this.tableCopiedIn.add(ref);
        			}
        		}
        	}
        }

    	// look through the fk, and add DBRefs for each of them.
    	for (ForeignKey fk:table.getForeignKeys()) {
			MutableDBRef ref = new MutableDBRef();
			ref.setParentTable(fk.getReferenceTableName());
			ref.setName(fk.getName());
			ref.setColumns(getColumnNames(fk.getColumns()));
			ref.setReferenceColumns(fk.getReferenceColumns());
			this.foreignKeys.put(getColumnNames(fk.getColumns()), ref);

			try {
				Table referenceTable = this.metadata.getTable(table.getParent().getFullName()+"."+fk.getReferenceTableName()); //$NON-NLS-1$
				boolean embedReferenceTbl = Boolean.parseBoolean(referenceTable.getProperty(EMBEDDABLE, false));
				if (embedReferenceTbl) {
					MutableDBRef pullKey = new MutableDBRef();
					pullKey.setParentTable(this.collectionTable.getName());
					pullKey.setColumns(getColumnNames(fk.getColumns()));
					pullKey.setReferenceColumns(fk.getReferenceColumns());
					pullKey.setEmbeddedTable(fk.getReferenceTableName());
					this.pullKeys.add(pullKey);
				}

				// if matches to 1to1 or many-to-one embedded scenario, build key to query the parent
				// document.
				if (embedInTableName != null && fk.getReferenceTableName().equalsIgnoreCase(embedInTableName)) {
					this.pushKey = new MutableDBRef();
					this.pushKey.setParentTable(embedInTableName);
					this.pushKey.setColumns(getColumnNames(fk.getColumns()));
					this.pushKey.setReferenceColumns(fk.getReferenceColumns());
					this.pushKey.setEmbeddedTable(this.collectionTable.getName());
					this.pushKey.setAssosiation(Assosiation.MANY);
					this.unwindTables.addLast(this.collectionTable.getName());

					// check to see if the parent table has relation to this table, if yes
					// then it is one-to-one, other wise many-to-one
					Table parentTable = this.metadata.getTable(table.getParent().getFullName()+"."+embedInTableName); //$NON-NLS-1$
					for (ForeignKey fk1:parentTable.getForeignKeys()) {
						if (fk1.getReferenceTableName().equals(table.getName())) {
							this.pushKey.setAssosiation(Assosiation.ONE);
							this.unwindTables.removeLast();
							break;
						}
					}

					// or for 1 to 1 to be true, fk columns are same as PK columns
					if (sameKeys(getColumnNames(fk.getColumns()), getColumnNames(table.getPrimaryKey().getColumns()))) {
						this.pushKey.setAssosiation(Assosiation.ONE);
						this.unwindTables.removeLast();
					}
				}
			} catch (TranslatorException e) {
				this.exceptions.add(e);
			}
    	}

        if (this.pushKey != null) {
        	this.collectionTable = this.metadata.getTable(table.getParent().getName(), embedInTableName);
        }
	}

	private boolean sameKeys(List<String> columns1, List<String> columns2) {
		if (columns1.size() != columns2.size()) {
			return false;
		}
		for (String name : columns1) {
			if (!columns2.contains(name)) {
				return false;
			}
		}
		return true;
	}

	boolean isPartOfPrimaryKey(NamedTable t, String columnName) {
		Table table = t.getMetadataObject();
		return isPartOfPrimaryKey(table, columnName);
	}

	boolean isPartOfPrimaryKey(Table table, String columnName) {
		KeyRecord pk = table.getPrimaryKey();
		if (pk != null) {
			for (Column column:pk.getColumns()) {
				if (getRecordName(column).equals(columnName)) {
					return true;
				}
			}
		}
		return false;
	}

	boolean hasCompositePrimaryKey(NamedTable t) {
		Table table = t.getMetadataObject();
		return hasCompositePrimaryKey(table);
	}

	boolean hasCompositePrimaryKey(Table table) {
		KeyRecord pk = table.getPrimaryKey();
		return pk.getColumns().size() > 1;
	}

	boolean isPartOfForeignKey(Table table, String columnName) {
		for (ForeignKey fk : table.getForeignKeys()) {
			for (Column column : fk.getColumns()) {
				if (column.getName().equals(columnName)) {
					return true;
				}
			}
		}
		return false;
	}

	boolean isMultiKeyForeignKey(Table table, String columnName) {
		for (ForeignKey fk : table.getForeignKeys()) {
			for (Column column : fk.getColumns()) {
				if (column.getName().equals(columnName)) {
					return fk.getColumns().size() > 1;
				}
			}
		}
		return false;
	}

	List<String> getColumnNames(List<Column> columns){
		ArrayList<String> names = new ArrayList<String>();
		for (Column c:columns) {
			names.add(c.getName());
		}
		return names;
	}
}
