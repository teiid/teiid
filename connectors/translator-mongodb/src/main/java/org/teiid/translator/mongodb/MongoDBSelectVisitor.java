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
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.teiid.language.*;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MutableDBRef.Assosiation;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MongoDBSelectVisitor extends HierarchyVisitor {
	public static final String MERGE = MetadataFactory.MONGO_URI+"MERGE"; //$NON-NLS-1$
	public static final String EMBEDDABLE = MetadataFactory.MONGO_URI+"EMBEDDABLE"; //$NON-NLS-1$

    private AtomicInteger aliasCount = new AtomicInteger();
	protected MongoDBExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	private Select command;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

	protected Stack<DBObject> onGoingPullCriteria = new Stack<DBObject>();
	protected Stack<Object> onGoingExpression  = new Stack<Object>();
	protected ConcurrentHashMap<Object, ColumnAlias> expressionMap = new ConcurrentHashMap<Object, ColumnAlias>();
	private HashMap<String, BasicDBObject> groupByProjections = new HashMap<String, BasicDBObject>();
	protected ColumnAlias onGoingAlias;
	protected MongoDocument mongoDoc;

	// derived stuff
	protected BasicDBObject project = new BasicDBObject();
	protected Integer limit;
	protected Integer skip;
	protected DBObject sort;
	protected DBObject match;
	protected DBObject having;
	protected BasicDBObject group = new BasicDBObject();
	protected ArrayList<String> selectColumns = new ArrayList<String>();
	protected ArrayList<String> selectColumnReferences = new ArrayList<String>();
	protected boolean projectBeforeMatch = false;
	protected LinkedList<String> unwindTables = new LinkedList<String>();
	protected ArrayList<Condition> pendingConditions = new ArrayList<Condition>();
	protected LinkedList<MongoDocument> joinedDocuments = new LinkedList<MongoDocument>();
	private boolean processingDerivedColumn = false;

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
		
		Expression originalExpr = obj.getExpression();
		
		this.processingDerivedColumn = true;
		append(originalExpr);
		this.processingDerivedColumn = false;

		Object expr = this.onGoingExpression.pop();

		ColumnAlias previousAlias = this.expressionMap.put(expr, this.onGoingAlias);
		if (previousAlias == null) {
			previousAlias = this.onGoingAlias;
		}

		if (originalExpr instanceof ColumnReference) {
			String elementName = getColumnName((ColumnReference)obj.getExpression());
			this.selectColumnReferences.add(elementName);
			// the the expression is already part of group by then the projection should be $_id.{name}
			BasicDBObject id = this.groupByProjections.get("_id"); //$NON-NLS-1$
			if (id == null) {
				if (this.command.isDistinct() || this.groupByProjections.get(previousAlias.projectedName) != null) {
					// this is DISTINCT case
					this.project.append(this.onGoingAlias.projectedName, "$_id."+previousAlias.projectedName); //$NON-NLS-1$
					this.selectColumns.add(this.onGoingAlias.projectedName);
					// if group by does not exist then build the group root id based on distinct
					this.group.put(this.onGoingAlias.projectedName, expr);
				}
				else {
					this.project.append(this.onGoingAlias.projectedName, expr);
					this.selectColumns.add(this.onGoingAlias.projectedName);					
				}
			}
			else {
				this.project.append(this.onGoingAlias.projectedName, id.get(previousAlias.projectedName)); 
				this.selectColumns.add(this.onGoingAlias.projectedName);					
			}			
		}
		else {
			if (originalExpr instanceof AggregateFunction) {
				ColumnAlias alias = addToProject(expr, false);
				if (!this.group.values().contains(expr)) {
					this.group.put(alias.projectedName, expr);
				}
			}
			else if (originalExpr instanceof Function) {
				addToProject(expr, true);
			}
			else if (originalExpr instanceof Condition) {
				// needs to be in the form "_mo: {$cond: [{$eq :["$city", "FREEDOM"]}, true, false]}}}"
				BasicDBList values = new BasicDBList();
				values.add(0, expr);
				values.add(1, true);
				values.add(2, false);
				addToProject(new BasicDBObject("$cond", values), true); //$NON-NLS-1$
			}
			// what user sees as project
			this.selectColumns.add(previousAlias.projectedName);
			this.selectColumnReferences.add(previousAlias.projectedName);
		}
		this.onGoingAlias = null;
	}

	private ColumnAlias buildAlias(String alias) {
		if (alias == null) {
			String str = "_m"+this.aliasCount.getAndIncrement(); //$NON-NLS-1$
			return new ColumnAlias(str, str, str, str, null);
		}
		return new ColumnAlias(alias, alias,alias,  alias, null);
	}

	@Override
	public void visit(ColumnReference obj) {
		try {
			String elementName = getColumnName(obj);

			if (obj.getMetadataObject() == null) {
				for (Object expr:this.expressionMap.keySet()) {
					ColumnAlias alias = this.expressionMap.get(expr);
					if (alias.projectedName.equals(elementName)) {
						this.onGoingExpression.push(expr);
						break;
					}
				}
			}
			else {
				String selectionName = elementName;
				String columnName = obj.getMetadataObject().getName();
				String pullColumnName = obj.getMetadataObject().getName();

				MongoDocument columnDocument = getDocument(obj.getTable().getMetadataObject());
				MongoDocument targetDocument = this.mongoDoc.getTargetDocument();
				String tableName = null;

				// column is on the same collection
				if (columnDocument.equals(targetDocument)) {
					// check if this is primary key
					if (columnDocument.isPartOfPrimaryKey(columnName)) {
						if (columnDocument.hasCompositePrimaryKey()) {
							elementName = "_id."+columnName; //$NON-NLS-1$
							selectionName = elementName;
							pullColumnName = "_id."+columnName; //$NON-NLS-1$
						}
						else {
							elementName = "_id"; //$NON-NLS-1$
							selectionName = elementName;
							pullColumnName = "_id"; //$NON-NLS-1$
						}
					}
					if (columnDocument.isPartOfForeignKey(columnName)) {
						// if this column is foreign key on same table then access must be "key.$id" because it is DBRef
						selectionName = elementName + ".$id"; //$NON-NLS-1$
						if (columnDocument.isMultiKeyForeignKey(columnName)) {
							selectionName = selectionName+"."+columnName; //$NON-NLS-1$
						}
						pullColumnName = columnName+".$id"; //$NON-NLS-1$
					}
				}
				else if (targetDocument.embeds(columnDocument)){
					// if this is embddable/embedIn table, then we need to use the embedded collection name
					MutableDBRef ref = targetDocument.getEmbeddedDocumentReferenceKey(columnDocument);
					elementName = ref.getName()+"."+columnName; //$NON-NLS-1$
					tableName = columnDocument.getTable().getName();

					if (columnDocument.isPartOfPrimaryKey(columnName)) {
						if (columnDocument.hasCompositePrimaryKey()) {
							elementName = ref.getName()+"."+"_id."+columnName; //$NON-NLS-1$ //$NON-NLS-2$
							selectionName = elementName;
							pullColumnName = "_id."+columnName; //$NON-NLS-1$
						}
						else {
							elementName = ref.getName()+"."+"_id"; //$NON-NLS-1$ //$NON-NLS-2$
							selectionName = elementName;
							pullColumnName = "_id"; //$NON-NLS-1$
						}
					}
					if (columnDocument.isPartOfForeignKey(columnName)) {
						// if this column is foreign key on same table then access must be "key.$id" because it is DBRef
						selectionName = elementName + ".$id"; //$NON-NLS-1$
						if (columnDocument.isMultiKeyForeignKey(columnName)) {
							selectionName = selectionName+"."+columnName; //$NON-NLS-1$
						}
						pullColumnName = columnName+".$id"; //$NON-NLS-1$
					}
				}

				String mongoExpr = "$"+elementName; //$NON-NLS-1$
				this.onGoingExpression.push(mongoExpr);

				if (this.onGoingAlias == null) {
					this.expressionMap.putIfAbsent(mongoExpr, new ColumnAlias(elementName, selectionName, columnName, pullColumnName, tableName));
				}
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
			return;
		}

	}

    private MongoDocument getDocument(Table table) {
    	if (this.mongoDoc != null && this.mongoDoc.getTable().getName().equals(table.getName())) {
    		return this.mongoDoc;
    	}
    	for (MongoDocument doc:this.joinedDocuments) {
    		if (doc.getTable().getName().equals(table.getName())) {
    			return doc;
    		}
    	}
		return null;
	}

	@Override
	public void visit(AggregateFunction obj) {
    	if (!obj.getParameters().isEmpty()) {
    		append(obj.getParameters());
    	}
    	else {
			// this is only true for count(*) case, so we need implicit group id clause
    		this.onGoingExpression.push(new Integer(1));
   			this.group.put("_id", null); //$NON-NLS-1$
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
			this.onGoingExpression.push(expr);
		}
    }

	private ColumnAlias addToProject(Object expr, boolean addExprAsProject) {
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

		if (this.project.get(previousAlias.projectedName) == null && !this.project.values().contains(expr)) {
			this.project.append(previousAlias.projectedName, addExprAsProject?expr:1);
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
			this.onGoingExpression.push(expr);
		}
	}

	@Override
	public void visit(NamedTable obj) {
		try {
			this.mongoDoc = new MongoDocument(obj.getMetadataObject(), this.metadata);
			configureUnwind(this.mongoDoc, null);
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}


	@Override
	public void visit(Join obj) {
		try {
			if (obj.getLeftItem() instanceof Join) {
				append(obj.getLeftItem());
				Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
				processJoin(this.mongoDoc, new MongoDocument(right, this.metadata), obj.getCondition());
			}
			else {
				Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
				Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
				processJoin(new MongoDocument(left, this.metadata), new MongoDocument(right, this.metadata), obj.getCondition());
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}

	private void configureUnwind(MongoDocument doc, String child) throws TranslatorException {
		if (doc.isMerged()) {
			MongoDocument mergeDoc = doc.getMergeDocument();
			if (mergeDoc.isMerged()) {
				configureUnwind(mergeDoc, doc.getTable().getName());
			}
			else {
				if (doc.getMergeAssosiation() == Assosiation.MANY) {
					if (child == null) {
						this.unwindTables.addFirst(doc.getMergeKey().getName());
					}
					else {
						this.unwindTables.addFirst(doc.getMergeKey().getName()+"."+child); //$NON-NLS-1$
						this.unwindTables.addFirst(doc.getMergeKey().getName());
					}
				}
			}
		}
	}

	private void processJoin(MongoDocument left, MongoDocument right, Condition cond) throws TranslatorException {
		if (left.embeds(right)) {
			this.mongoDoc = left;
			this.joinedDocuments.add(right);
			configureUnwind(right, null);
		}
		else if (right.embeds(left)) {
			this.mongoDoc = right;
			this.joinedDocuments.add(right);
			configureUnwind(right, null);
		}
		else {
			if (this.mongoDoc != null) {
				// this is for nested grand kids
				for (MongoDocument child:this.joinedDocuments) {
					if (child.embeds(right)) {
						this.joinedDocuments.add(right);
						configureUnwind(right, null);
						return;
					}
				}
			}
			throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18012, left.getTable().getName(), right.getTable().getName()));
		}

        if (cond != null) {
        	this.pendingConditions.add(cond);
        }
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

        if (!this.onGoingExpression.isEmpty()) {
        	if (this.match != null) {
        		this.match = QueryBuilder.start().and(this.match, (DBObject)this.onGoingExpression.pop()).get();
        	}
        	else {
        		this.match = (DBObject)this.onGoingExpression.pop();
        	}
        }

        if (obj.getGroupBy() != null) {
            append(obj.getGroupBy());
        }

    	append(obj.getDerivedColumns());

    	// in distinct since there may not be group by, but mongo requires a grouping clause.
		if (obj.getGroupBy() == null && obj.isDistinct() && !this.group.containsField("_id")) { //$NON-NLS-1$ 
			BasicDBObject id = new BasicDBObject(this.group);
			this.group.clear();
			this.group.put("_id", id); //$NON-NLS-1$
		}

        if (obj.getHaving() != null) {
            append(obj.getHaving());
        }

        if (!this.onGoingExpression.isEmpty()) {
        	this.having = (DBObject)this.onGoingExpression.pop();
        }

        if (!this.group.isEmpty()) {
            if (this.group.get("_id") == null) { //$NON-NLS-1$
            	this.group.put("_id", null); //$NON-NLS-1$
            }
        }
        else {
        	this.group = null;
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
        
		// this for $cond in the select statement, and formatting of command for $cond vs $match is different
        if (this.processingDerivedColumn) {
        	visitDerivedExpression(obj);
        	return;
        }
        
        // this for the normal where clause
		ColumnAlias exprAlias = getExpressionAlias(obj.getLeftExpression());

        append(obj.getRightExpression());

        Object rightExpr = this.onGoingExpression.pop();
        if (this.expressionMap.get(rightExpr) != null) {
        	rightExpr = this.expressionMap.get(rightExpr).projectedName;
        }
    	
		QueryBuilder query = QueryBuilder.start(exprAlias.selectionName);
		QueryBuilder pullQuery = QueryBuilder.start(exprAlias.pullColumnName);
    	
    	switch(obj.getOperator()) {
        case EQ:
        	query.is(rightExpr);
        	pullQuery.is(rightExpr);
        	break;
        case NE:
        	query.notEquals(rightExpr);
        	pullQuery.notEquals(rightExpr);
        	break;
        case LT:
        	query.lessThan(rightExpr);
        	pullQuery.lessThan(rightExpr);
        	break;
        case LE:
        	query.lessThanEquals(rightExpr);
        	pullQuery.lessThanEquals(rightExpr);
        	break;
        case GT:
        	query.greaterThan(rightExpr);
        	pullQuery.greaterThan(rightExpr);
        	break;
        case GE:
        	query.greaterThanEquals(rightExpr);
        	pullQuery.greaterThanEquals(rightExpr);
        	break;
        }
    	this.onGoingExpression.push(query.get());
    	
        if (obj.getLeftExpression() instanceof ColumnReference) {
        	ColumnReference colum = (ColumnReference)obj.getLeftExpression();
			this.mongoDoc.updateReferenceColumnValue(colum.getTable().getName(), exprAlias.columnName, rightExpr);
        }

        // build pull criteria for delete
       	this.onGoingPullCriteria.push(pullQuery.get());
	}

	private void visitDerivedExpression(Comparison obj) {
		append(obj.getLeftExpression());
		Object leftExpr = this.onGoingExpression.pop();
		append(obj.getRightExpression());
		Object rightExpr = this.onGoingExpression.pop();
		
		BasicDBList values = new BasicDBList();
		values.add(0, leftExpr);
		values.add(1, rightExpr);

		switch(obj.getOperator()) {
		case EQ:
			this.onGoingExpression.push(new BasicDBObject("$eq", values)); //$NON-NLS-1$
			this.onGoingPullCriteria.push(new BasicDBObject("$eq", values)); //$NON-NLS-1$
			break;
		case NE:
			this.onGoingExpression.push(new BasicDBObject("$ne", values)); //$NON-NLS-1$
			this.onGoingPullCriteria.push(new BasicDBObject("$ne", values)); //$NON-NLS-1$
			break;
		case LT:
			this.onGoingExpression.push(new BasicDBObject("$lt", values)); //$NON-NLS-1$
			this.onGoingPullCriteria.push(new BasicDBObject("$lt", values)); //$NON-NLS-1$
			break;
		case LE:
			this.onGoingExpression.push(new BasicDBObject("$lte", values)); //$NON-NLS-1$
			this.onGoingPullCriteria.push(new BasicDBObject("$lte", values)); //$NON-NLS-1$
			break;
		case GT:
			this.onGoingExpression.push(new BasicDBObject("$gt", values)); //$NON-NLS-1$
			this.onGoingPullCriteria.push(new BasicDBObject("$gt", values)); //$NON-NLS-1$
			break;
		case GE:
			this.onGoingExpression.push(new BasicDBObject("$gte", values)); //$NON-NLS-1$
			this.onGoingPullCriteria.push(new BasicDBObject("$gte", values)); //$NON-NLS-1$
			break;
		}
	}

	@Override
    public void visit(AndOr obj) {
		append(obj.getLeftCondition());
		append(obj.getRightCondition());
        DBObject right = (DBObject)this.onGoingExpression.pop();
        DBObject left = (DBObject) this.onGoingExpression.pop();

        DBObject pullRight = this.onGoingPullCriteria.pop();
        DBObject pullLeft = this.onGoingPullCriteria.pop();

        switch(obj.getOperator()) {
        case AND:
        	this.onGoingExpression.push(QueryBuilder.start().and(left, right).get());
        	this.onGoingPullCriteria.push(QueryBuilder.start().and(pullLeft, pullRight).get());
        	break;
        case OR:
        	this.onGoingExpression.push(QueryBuilder.start().or(left, right).get());
        	this.onGoingPullCriteria.push(QueryBuilder.start().or(pullLeft, pullRight).get());
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
		ColumnAlias exprAlias = getExpressionAlias(obj.getLeftExpression());
		QueryBuilder query = QueryBuilder.start(exprAlias.selectionName);
		QueryBuilder pullQuery = QueryBuilder.start(exprAlias.pullColumnName);

    	append(obj.getRightExpressions());

        BasicDBList values = new BasicDBList();
        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
        	values.add(0, this.onGoingExpression.pop());
        }

		if (query != null) {
			if (obj.isNegated()) {
				query.notIn(values);
				pullQuery.notIn(values);
			} else {
				query.in(values);
				pullQuery.in(values);
			}
			this.onGoingExpression.push(query.get());
			this.onGoingPullCriteria.push(pullQuery.get());
		}
	}

	private ColumnAlias getExpressionAlias(Expression obj) {
		// the way DBRef names handled in projection vs selection is different.
		// in projection we want to see as "col" mapped to "col.$_id" as it is treated as sub-document
		// where as in selection it will should be "col._id".
		append(obj);

		Object expr = this.onGoingExpression.pop();
		ColumnAlias exprAlias = this.expressionMap.get(expr);
		if (exprAlias == null) {
			exprAlias = buildAlias(null);
			this.expressionMap.put(expr, exprAlias);
		}

		// when expression shows up in a condition, but it is not a derived column
		// then add implicit project on that alias.
		return exprAlias;
	}

	@Override
	public void visit(IsNull obj) {
		ColumnAlias exprAlias = getExpressionAlias(obj.getExpression());
		QueryBuilder query = QueryBuilder.start(exprAlias.selectionName);
		QueryBuilder pullQuery = QueryBuilder.start(exprAlias.pullColumnName);

		if (query != null) {
			if (obj.isNegated()) {
				query.notEquals(null);
				pullQuery.notEquals(null);
			}
			else {
				query.is(null);
				pullQuery.is(null);
			}
			this.onGoingExpression.push(query.get());
			this.onGoingPullCriteria.push(pullQuery.get());
		}
	}

	@Override
	public void visit(Like obj) {
		ColumnAlias exprAlias = getExpressionAlias(obj.getLeftExpression());
		QueryBuilder query = QueryBuilder.start(exprAlias.selectionName);
		QueryBuilder pullQuery = QueryBuilder.start(exprAlias.pullColumnName);

		if (query != null) {
			if (obj.isNegated()) {
				query.not();
				pullQuery.not();
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
			query.is(Pattern.compile(regex));
			pullQuery.is(Pattern.compile(regex));
			this.onGoingExpression.push(query.get());
			this.onGoingPullCriteria.push(pullQuery.get());
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
			this.sort =  new BasicDBObject(alias.projectedName, (obj.getOrdering() == Ordering.ASC)?1:-1);
		}
		else {
			this.sort.put(alias.projectedName, (obj.getOrdering() == Ordering.ASC)?1:-1);
		}
	}

	@Override
	public void visit(GroupBy obj) {
		if (obj.getElements().size() == 1) {
			append(obj.getElements().get(0));
			Object mongoExpr = this.onGoingExpression.pop();
			ColumnAlias alias = this.expressionMap.get(mongoExpr);
			this.group.put("_id", mongoExpr); //$NON-NLS-1$
			this.groupByProjections.put("_id", new BasicDBObject(alias.projectedName, "$_id")); //$NON-NLS-1$ //$NON-NLS-2$
		}
		else {
			BasicDBObject fields = new BasicDBObject();
			BasicDBObject exprs = new BasicDBObject();
			for (Expression expr : obj.getElements()) {
				append(expr);
				Object mongoExpr = this.onGoingExpression.pop();
				ColumnAlias alias = this.expressionMap.get(mongoExpr);
				exprs.put(alias.projectedName, mongoExpr);
				fields.put(alias.projectedName, "$_id."+alias.projectedName); //$NON-NLS-1$
			}
			this.group.put("_id", exprs); //$NON-NLS-1$
			this.groupByProjections.put("_id", fields); //$NON-NLS-1$
		}
	}

	static boolean isPartOfPrimaryKey(Table table, String columnName) {
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

	boolean hasCompositePrimaryKey(Table table) {
		KeyRecord pk = table.getPrimaryKey();
		return pk.getColumns().size() > 1;
	}

	static boolean isPartOfForeignKey(Table table, String columnName) {
		for (ForeignKey fk : table.getForeignKeys()) {
			for (Column column : fk.getColumns()) {
				if (column.getName().equals(columnName)) {
					return true;
				}
			}
		}
		return false;
	}

	static String getForeignKeyRefTable(Table table, String columnName) {
		for (ForeignKey fk : table.getForeignKeys()) {
			for (Column column : fk.getColumns()) {
				if (column.getName().equals(columnName)) {
					return fk.getReferenceTableName();
				}
			}
		}
		return null;
	}

	static List<String> getColumnNames(List<Column> columns){
		ArrayList<String> names = new ArrayList<String>();
		for (Column c:columns) {
			names.add(c.getName());
		}
		return names;
	}
}
