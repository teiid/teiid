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

import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.Array;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.In;
import org.teiid.language.IsNull;
import org.teiid.language.Join;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.Select;
import org.teiid.language.SortSpecification;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MutableDBRef.Association;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MongoDBSelectVisitor extends HierarchyVisitor {
    private AtomicInteger aliasCount = new AtomicInteger();
    private AtomicInteger columnCount = new AtomicInteger();
	protected MongoDBExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	private Select command;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

	protected Stack<Object> onGoingExpression  = new Stack<Object>();
	protected ConcurrentHashMap<Object, ColumnDetail> expressionMap = new ConcurrentHashMap<Object, ColumnDetail>();
	private HashMap<String, BasicDBObject> groupByProjections = new HashMap<String, BasicDBObject>();
	protected MongoDocument mongoDoc;

	// derived stuff
	protected BasicDBObject project = new BasicDBObject();
	protected BasicDBObject unwindProject;
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
		Expression teiidExpression = obj.getExpression();
		String alias = getAlias(obj.getAlias()); 
		
		this.processingDerivedColumn = true;
		append(teiidExpression);
		
		Object mongoExpression = this.onGoingExpression.pop();

		ColumnDetail exprDetails = this.expressionMap.get(mongoExpression);
		if (exprDetails == null) {
			exprDetails = new ColumnDetail();
			exprDetails.projectedName = alias;
			this.expressionMap.put(mongoExpression, exprDetails);
		}
		
		// the the expression is already part of group by then the projection should be $_id.{name}
		this.selectColumns.add(alias);
		if (exprDetails.partOfGroupBy) {
			BasicDBObject id = this.groupByProjections.get("_id"); //$NON-NLS-1$
			this.project.append(alias, id.get(exprDetails.projectedName));
			exprDetails.projectedName = alias;
	        this.selectColumnReferences.add(alias);
		}
		else {
			exprDetails.projectedName = alias;
			exprDetails.partOfProject = true;
			if (teiidExpression instanceof ColumnReference) {
				String elementName = getColumnName((ColumnReference)obj.getExpression());
				this.selectColumnReferences.add(elementName);
				// the the expression is already part of group by then the projection should be $_id.{name}
				if (this.command.isDistinct() || this.groupByProjections.get(exprDetails.projectedName) != null) {
					// this is DISTINCT case
					this.project.append(alias, "$_id."+exprDetails.projectedName); //$NON-NLS-1$
					// if group by does not exist then build the group root id based on distinct
					this.group.put(alias, mongoExpression);
				}
				else {
					this.project.append(alias, mongoExpression);
				}			
			}
			else {
			    implicitProject(teiidExpression, mongoExpression, exprDetails);
				// what user sees as project
				this.selectColumnReferences.add(exprDetails.projectedName);
			}
		}
		this.processingDerivedColumn = false;		
	}

	private String getAlias(String alias) {
		if (alias == null) {
			return "_m"+this.aliasCount.getAndIncrement(); //$NON-NLS-1$
		}
		return alias;
	}
	
	private ColumnDetail buildAlias() {
	    return buildAlias("_m"+this.aliasCount.getAndIncrement()); //$NON-NLS-1$
	}
	
	private ColumnDetail buildAlias(String alias) {
	    if (alias == null) {
	        return buildAlias();
	    }
	    ColumnDetail detail =  new ColumnDetail();
		detail.projectedName = alias;
		return detail;
	}

	@Override
	public void visit(ColumnReference obj) {
		try {
			if (obj.getMetadataObject() == null) {
				for (Object expr:this.expressionMap.keySet()) {
					ColumnDetail columnInfo = this.expressionMap.get(expr);
					if (columnInfo.projectedName.equals(getColumnName(obj))) {
						this.onGoingExpression.push(expr);
						break;
					}
				}
			}
			else {
			    // do not allow array type in where clauses etc.
				/*
			    if (!this.processingDerivedColumn) {
			        if (DataTypeManager.isArrayType(obj.getMetadataObject().getRuntimeType())){
			            this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18027, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18027, getColumnName(obj))));
			        }
			    }
			    */

			    ColumnDetail columnInfo = buildColumnDetail(obj);
				Object mongoExpr = columnInfo.expression; 
				this.onGoingExpression.push(mongoExpr);
				this.expressionMap.putIfAbsent(mongoExpr, columnInfo); 
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
			return;
		}
	}
	
	ColumnDetail buildColumnDetail(ColumnReference obj) throws TranslatorException {
        MongoDocument columnDocument = getDocument(obj.getTable().getMetadataObject());
        MongoDocument targetDocument = this.mongoDoc.getTargetDocument();
        
        String columnName = obj.getMetadataObject().getName();
        String documentFieldName = obj.getMetadataObject().getName();
        String documentQueryFieldName = obj.getMetadataObject().getName();
        String targetDocumentFieldName = obj.getMetadataObject().getName();
        String tableName = columnDocument.getTable().getName();
        String parentTableName = columnDocument.getTable().getName();
        
        // column is on the same collection
        if (columnDocument.equals(targetDocument)) {
            // check if this is primary key
            if (columnDocument.isPartOfPrimaryKey(columnName)) {
                if (columnDocument.hasCompositePrimaryKey()) {
                    documentFieldName = "_id."+columnName; //$NON-NLS-1$
                    documentQueryFieldName = documentFieldName;
                    targetDocumentFieldName = documentFieldName;
                }
                else {
                    documentFieldName = "_id"; //$NON-NLS-1$
                    documentQueryFieldName = documentFieldName;
                    targetDocumentFieldName = documentFieldName;
                }
            }
        }
        else if (targetDocument.embeds(columnDocument)){
            // if this is embddable table, then we need to use the embedded collection name
            MutableDBRef ref = targetDocument.getEmbeddedDocumentReferenceKey(columnDocument);
            documentFieldName = ref.getAlias()+"."+columnName; //$NON-NLS-1$
            documentQueryFieldName = documentFieldName;
            
            if (columnDocument.isPartOfPrimaryKey(columnName)) {
            	// if this is primary key then key must be in parent document
                if (columnDocument.hasCompositePrimaryKey()) {
                    documentFieldName = ref.getAlias()+"."+"_id."+columnName; //$NON-NLS-1$ //$NON-NLS-2$
                    documentQueryFieldName = ref.getParentColumnName(columnName);
                    targetDocumentFieldName = ref.getParentColumnName(columnName);                            
                }
                else {
                    documentFieldName = ref.getAlias()+"."+"_id"; //$NON-NLS-1$ //$NON-NLS-2$
                    documentQueryFieldName = ref.getParentColumnName(columnName);
                    targetDocumentFieldName = ref.getParentColumnName(columnName);
                }                       
            }
            if (columnDocument.isPartOfForeignKey(columnName)) {
                // if this column is foreign key on same table then access must be "key.$id" because it is DBRef
                documentQueryFieldName = ref.getParentColumnName(columnName);
                targetDocumentFieldName = ref.getParentColumnName(columnName);
            }
        }
        else if (targetDocument.merges(columnDocument)){
            // if this is merge table, then we need to use the merge collection name
            MutableDBRef ref = targetDocument.getEmbeddedDocumentReferenceKey(columnDocument);
            documentFieldName = ref.getAlias()+"."+columnName; //$NON-NLS-1$
            documentQueryFieldName = documentFieldName;

            // one-2-one
            if (columnDocument.isPartOfPrimaryKey(columnName) && 
                    ref.getAssociation() == MutableDBRef.Association.ONE) {
                documentFieldName = "_id"; //$NON-NLS-1$
                if (columnDocument.hasCompositePrimaryKey()) {
                    documentFieldName = "_id."+columnName; //$NON-NLS-1$
                }
                documentQueryFieldName = documentFieldName;
                targetDocumentFieldName = documentFieldName;     
                parentTableName = targetDocument.getTable().getName();
            }
            else if (columnDocument.isPartOfPrimaryKey(columnName) && 
                    ref.getAssociation() == MutableDBRef.Association.MANY) {
                if (columnDocument.hasCompositePrimaryKey()) {
                    documentFieldName = ref.getAlias()+"."+"_id."+columnName; //$NON-NLS-1$ //$NON-NLS-2$
                    documentQueryFieldName = documentFieldName;
                    targetDocumentFieldName = "_id."+columnName; //$NON-NLS-1$
                }
                else {
                    documentFieldName = ref.getAlias()+"."+"_id"; //$NON-NLS-1$ //$NON-NLS-2$
                    documentQueryFieldName = documentFieldName;
                    targetDocumentFieldName = "_id"; //$NON-NLS-1$
                }
            }

            if (columnDocument.isPartOfForeignKey(columnName)) {
                String parentColumnName = ref.getParentColumnName(columnName);
                if (parentColumnName != null && targetDocument.isPartOfPrimaryKey(parentColumnName)) {
                    documentFieldName = "_id"; //$NON-NLS-1$
                    if (ref.isNested()) {
                        documentFieldName = ref.getParentTable()+"._id"; //$NON-NLS-1$
                    }
                    if (columnDocument.isCompositeForeignKey(columnName)) {
                        documentFieldName = "_id."+columnName; //$NON-NLS-1$
                    }
                    documentQueryFieldName = documentFieldName;
                    targetDocumentFieldName = parentColumnName;  
                    parentTableName = ref.getParentTable();
                }
            }
        }	   
        ColumnDetail detail = new ColumnDetail();
        detail.documentFieldName = documentFieldName;
        detail.projectedName = documentFieldName;
        detail.documentQueryFieldName = documentQueryFieldName;
        detail.columnName = columnName;
        detail.targetDocumentFieldName = targetDocumentFieldName;
        detail.tableName=tableName;
        detail.targetDocumentName = parentTableName;
        detail.expression = "$"+documentFieldName; //$NON-NLS-1$
        return detail;
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

    	BasicDBObject expr = null;
		if (obj.getName().equals(AggregateFunction.COUNT)) {
	        // this is only true for count(*) case, so we need implicit group id clause
		    this.group.put("_id", null); //$NON-NLS-1$
			expr = new BasicDBObject("$sum", new Integer(1)); //$NON-NLS-1$
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

	private ColumnDetail addToProject(Object expr, boolean addExprAsProject, ColumnDetail detail, boolean needsProjection) {
		if (detail == null) {
			// if expression is in having/where clause there is will be no alias; however mongo expects some functions
			// to be elevated to project before $match can be run
			if (needsProjection) {
				this.projectBeforeMatch = true;
			}
			detail = buildAlias();
			this.expressionMap.putIfAbsent(expr, detail);
		}
		detail.expression = expr;
		
		if (needsProjection) {
			if (this.project.get(detail.projectedName) == null && !this.project.values().contains(expr)) {
				this.project.append(detail.projectedName, addExprAsProject?expr:1);			
			}
			detail.partOfProject = true;
		}
		else {
			detail.partOfProject = false;
		}
		return detail;
	}

	@Override
	public void visit(Function obj) {
		String functionName = obj.getName();
		if (functionName.indexOf('.') != -1) {
			functionName = functionName.substring(functionName.indexOf('.')+1);
		}
    	if (this.executionFactory.getFunctionModifiers().containsKey(functionName)) {
            List<?> parts =  this.executionFactory.getFunctionModifiers().get(functionName).translate(obj);
            if (parts != null) {
            	obj = (Function)parts.get(0);
            }
    	}
    	BasicDBObject expr = null;
    	if (isGeoSpatialFunction(functionName)) {
			expr = (BasicDBObject)handleGeoSpatialFunction(functionName, obj);
    	}
    	else {
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
    	}

		if(expr != null) {			
			this.onGoingExpression.push(expr);
		}
	}

	private boolean isGeoSpatialFunction(String name) {
		for (String func:MongoDBExecutionFactory.GEOSPATIAL_FUNCTIONS) {
			if (name.equalsIgnoreCase(func)) {
				return true;
			}
		}
		return false;
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
				processJoin(this.mongoDoc, new MongoDocument(right, this.metadata), obj.getCondition(), (Join)obj.getLeftItem());
			}
			else if (obj.getRightItem() instanceof Join) {
				Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
				append(obj.getRightItem());
				processJoin(this.mongoDoc, new MongoDocument(left, this.metadata), obj.getCondition(), (Join)obj.getRightItem());
			}
			else {
				Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
				Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
				processJoin(new MongoDocument(left, this.metadata), new MongoDocument(right, this.metadata), obj.getCondition(), obj);
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}

	//TODO: test nested merge? does it still work?
	private void configureUnwind(MongoDocument mergeDocument, String child) throws TranslatorException {
		if (mergeDocument.isMerged()) {
			MongoDocument parentDocument = mergeDocument.getMergeDocument();
			if (parentDocument.isMerged()) {
				configureUnwind(parentDocument, mergeDocument.getMergeKey().getAlias());
			}
			else {
				if (mergeDocument.getMergeAssociation() == Association.MANY) {
					if (child == null) {
						this.unwindTables.addFirst(mergeDocument.getMergeKey().getAlias());
					}
					else {
						this.unwindTables.addFirst(mergeDocument.getMergeKey().getAlias()+"."+child); //$NON-NLS-1$
						this.unwindTables.addFirst(mergeDocument.getMergeKey().getAlias());
					}
				}
			}
		}
	}

	private void processJoin(MongoDocument left, MongoDocument right, Condition cond, Join join) throws TranslatorException {
		// now adjust for the left/right outer depending upon who is the outer document
		JoinCriteriaVisitor jcv = new JoinCriteriaVisitor(join, left, right);
		DBObject match =  jcv.getCondition();
		BasicDBObject projection =  jcv.getProjection();
		
    	if (match != null && this.match != null) {    		    	
    		this.match = QueryBuilder.start().and(this.match, match).get();
    	}
    	else {
    		this.match = match;
    	}
    	
    	if (projection != null) {
    		if (this.unwindProject == null) {
    			this.unwindProject = projection;
    		}
    		else {
    			this.unwindProject.append(jcv.getAliasName(), projection.get(jcv.getAliasName()));
    		}
    	}
		
		if (left.contains(right)) {
			this.mongoDoc = left;
			this.joinedDocuments.add(right);
			configureUnwind(right, null);
		}
		else if (right.contains(left)) {
			this.mongoDoc = right;
			this.joinedDocuments.add(left);
			configureUnwind(left, null);
		}
		else {
			if (this.mongoDoc != null) {
				// this is for nested grand kids
				for (MongoDocument child:this.joinedDocuments) {
					if (child.contains(right)) {
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
	    
	    try {
            SQLRewriterVisitor.rewrite(obj);
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
	    
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
        		DBObject expr = (DBObject)this.onGoingExpression.pop();
        		ArrayList exprs = (ArrayList)expr.get("$and"); //$NON-NLS-1$
        		if (exprs != null) {
        			exprs.add(0, this.match);
        			this.match = expr;
        		}
        		else {
        			this.match = QueryBuilder.start().and(this.match, expr).get();
        		}
        	}
        	else {
        		this.match = (DBObject)this.onGoingExpression.pop();
        	}
        }
        else {
        	// default match in case no where clause used
        	// TEIID-2841 - in ONE-2-ONE case $unwind works as filter
        	if (this.mongoDoc.isMerged() && this.mongoDoc.getMergeAssociation().equals(Association.ONE)) {
        		this.match = QueryBuilder.start(mongoDoc.getTable().getName()).exists("true").get(); //$NON-NLS-1$
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
	
	private ColumnDetail implicitProject(Expression teiidExpr, Object mongoExpr, ColumnDetail providedAlias) {
        if (teiidExpr instanceof ColumnReference) {
            return this.expressionMap.get(mongoExpr);
        }
        else if (teiidExpr instanceof AggregateFunction) {
            ColumnDetail alias = addToProject(mongoExpr, false, providedAlias, true);
            if (!this.group.values().contains(mongoExpr)) {
                this.group.put(alias.projectedName, mongoExpr);
            }
            return alias;
        }
        else if (teiidExpr instanceof Function) {
        	Boolean avoidProjection = Boolean.valueOf(((Function) teiidExpr).getMetadataObject().getProperty(MongoDBExecutionFactory.AVOID_PROJECTION, false));
            return addToProject(mongoExpr, true, providedAlias, processingDerivedColumn||!avoidProjection);
        }
        else if (teiidExpr instanceof Condition) {
            // needs to be in the form "_mo: {$cond: [{$eq :["$city", "FREEDOM"]}, true, false]}}}"
            BasicDBList values = new BasicDBList();
            values.add(0, mongoExpr);
            values.add(1, true);
            values.add(2, false);
            return addToProject(new BasicDBObject("$cond", values), true, providedAlias, true); //$NON-NLS-1$
        }
        else if (teiidExpr instanceof Literal) {
            if (this.executionFactory.getVersion().compareTo(MongoDBExecutionFactory.TWO_6) >= 0) {
                return addToProject(new BasicDBObject("$literal", mongoExpr), true, providedAlias, true); //$NON-NLS-1$
            }
            this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18026, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18026)));
        }
        return null;
	}

	@Override
	public void visit(Comparison obj) {
        
		// this for $cond in the select statement, and formatting of command for $cond vs $match is different
        if (this.processingDerivedColumn) {
        	visitDerivedExpression(obj);
        	return;
        }
        
        // this for the normal where clause
		ColumnDetail leftExprDetails = getExpressionAlias(obj.getLeftExpression());
        append(obj.getRightExpression());
        Object rightExpr = this.onGoingExpression.pop();
        if (this.expressionMap.get(rightExpr) != null) {
        	rightExpr = this.expressionMap.get(rightExpr).projectedName;
        }
        
        QueryBuilder query = leftExprDetails.getQueryBuilder();
        buildComparisionQuery(obj, rightExpr, query);
		
		if (leftExprDetails.partOfProject || obj.getLeftExpression() instanceof ColumnReference) {
	    	this.onGoingExpression.push(query.get());   	
		}
		else {
			this.onGoingExpression.push(buildFunctionQuery(obj, (BasicDBObject)leftExprDetails.expression, rightExpr)); 
		}
		
    	if (obj.getLeftExpression() instanceof ColumnReference) {
            ColumnReference colum = (ColumnReference)obj.getLeftExpression();
            this.mongoDoc.updateReferenceColumnValue(colum.getTable().getName(), leftExprDetails.columnName, rightExpr);
        }		
	}
	
	protected BasicDBObject buildFunctionQuery(Comparison obj, BasicDBObject leftExpr, Object rightExpr) {
        switch(obj.getOperator()) {
        case EQ:
        	if (rightExpr instanceof Boolean && ((Boolean)rightExpr)) {
        		return leftExpr;
        	}
			//$FALL-THROUGH$
		case NE:
        case LT:
        case LE:
        case GT:
        case GE:
        }
        this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18030, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18030)));
        return null; 
    }	
	
	protected void buildComparisionQuery(Comparison obj, Object rightExpr, QueryBuilder query) {
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
			break;
		case NE:
			this.onGoingExpression.push(new BasicDBObject("$ne", values)); //$NON-NLS-1$
			break;
		case LT:
			this.onGoingExpression.push(new BasicDBObject("$lt", values)); //$NON-NLS-1$
			break;
		case LE:
			this.onGoingExpression.push(new BasicDBObject("$lte", values)); //$NON-NLS-1$
			break;
		case GT:
			this.onGoingExpression.push(new BasicDBObject("$gt", values)); //$NON-NLS-1$
			break;
		case GE:
			this.onGoingExpression.push(new BasicDBObject("$gte", values)); //$NON-NLS-1$
			break;
		}
	}

	@Override
    public void visit(AndOr obj) {
		append(obj.getLeftCondition());
		append(obj.getRightCondition());
        DBObject right = (DBObject)this.onGoingExpression.pop();
        DBObject left = (DBObject) this.onGoingExpression.pop();

        switch(obj.getOperator()) {
        case AND:
        	this.onGoingExpression.push(QueryBuilder.start().and(left, right).get());
        	break;
        case OR:
        	this.onGoingExpression.push(QueryBuilder.start().or(left, right).get());
        	break;
        }       
    }
	
	@Override
    public void visit(Array array) {
	    append(array.getExpressions());
	    BasicDBList values = new BasicDBList();
	    for (int i = 0; i < array.getExpressions().size(); i++) {
	        values.add(0, this.onGoingExpression.pop());
	    }
	    this.onGoingExpression.push(values);
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
		append(obj.getLeftExpression());
		Object expr = this.onGoingExpression.pop();
		ColumnDetail detail = this.expressionMap.get(expr);
		QueryBuilder query = QueryBuilder.start();
		if (detail == null) {
	        this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18031, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18031)));
		}
		else {
			query = detail.getQueryBuilder();
			this.onGoingExpression.push(buildInQuery(obj, query).get());
		}
	}
	
    protected QueryBuilder buildInQuery(In obj, QueryBuilder query) {
        append(obj.getRightExpressions());
        BasicDBList values = new BasicDBList();
        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
            values.add(0, this.onGoingExpression.pop());
        }
        if (obj.isNegated()) {
            query.notIn(values);
        } else {
            query.in(values);
        }
        return query;
    }	

	ColumnDetail getExpressionAlias(Expression obj) {
		// the way DBRef names handled in projection vs selection is different.
		// in projection we want to see as "col" mapped to "col.$_id" as it is treated as sub-document
		// where as in selection it will should be "col._id".
		append(obj);

		Object expr = this.onGoingExpression.pop();
		ColumnDetail detail = implicitProject(obj, expr, this.expressionMap.get(expr));

		// when expression shows up in a condition, but it is not a derived column
		// then add implicit project on that alias.
		return detail;
	}

	@Override
	public void visit(IsNull obj) {
		append(obj.getExpression());
		Object expr = this.onGoingExpression.pop();
		ColumnDetail detail = this.expressionMap.get(expr);
		QueryBuilder query = QueryBuilder.start();
		if (detail == null) {
	        this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18032, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18032)));
		}
		else {
			query = detail.getQueryBuilder();
			this.onGoingExpression.push(buildIsNullQuery(obj, query).get());
		}
	}
	
    protected QueryBuilder buildIsNullQuery(IsNull obj, QueryBuilder query) {
        if (obj.isNegated()) {
            query.notEquals(null);
        }
        else {
            query.is(null);
        }
        return query;
    }	

	@Override
	public void visit(Like obj) {
		append(obj.getLeftExpression());
		Object expr = this.onGoingExpression.pop();
		ColumnDetail detail = this.expressionMap.get(expr);
		QueryBuilder query = QueryBuilder.start();
		if (detail == null) {
			this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18033, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18033)));
		}
		else {
			query = detail.getQueryBuilder();
			buildLikeQuery(obj, query);
			this.onGoingExpression.push(query.get());
		}
	}

    protected QueryBuilder buildLikeQuery(Like obj, QueryBuilder query) {
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
		query.is(Pattern.compile(regex));	
		return query;
    }

	@Override
	public void visit(Limit obj) {
	    if (obj.getRowLimit() != Integer.MAX_VALUE) {
	        this.limit = new Integer(obj.getRowLimit());
	    }
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
		ColumnDetail alias = this.expressionMap.get(expr);
		if (this.sort == null) {
			this.sort =  new BasicDBObject(alias.projectedName, (obj.getOrdering() == Ordering.ASC)?1:-1);
		}
		else {
			this.sort.put(alias.projectedName, (obj.getOrdering() == Ordering.ASC)?1:-1);
		}
	}

	@Override
	public void visit(GroupBy obj) {
		// since grouping requires additional step, this is done at a different pipeline stage. 
		// so, that requires additional in-direction.		
		if (obj.getElements().size() == 1) {
			append(obj.getElements().get(0));
			Object mongoExpr = this.onGoingExpression.pop();
			ColumnDetail alias = this.expressionMap.get(mongoExpr);
			alias.projectedName = "_c"+this.columnCount.getAndIncrement(); //$NON-NLS-1$
			this.group.put("_id", new BasicDBObject(alias.projectedName, mongoExpr)); //$NON-NLS-1$
			this.groupByProjections.put("_id", new BasicDBObject(alias.projectedName, "$_id."+alias.projectedName)); //$NON-NLS-1$ //$NON-NLS-2$
			alias.partOfGroupBy = true;
		}
		else {
			BasicDBObject fields = new BasicDBObject();
			BasicDBObject exprs = new BasicDBObject();
			for (Expression expr : obj.getElements()) {
				append(expr);
				Object mongoExpr = this.onGoingExpression.pop();
				ColumnDetail alias = this.expressionMap.get(mongoExpr);
				alias.projectedName = "_c"+this.columnCount.getAndIncrement(); //$NON-NLS-1$
				exprs.put(alias.projectedName, mongoExpr);
				fields.put(alias.projectedName, "$_id."+alias.projectedName); //$NON-NLS-1$
				alias.partOfGroupBy = true;
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
	
	static enum SpatialType {Point, LineString, Polygon, MultiPoint, MultiLineString};
	
	private DBObject handleGeoSpatialFunction(String functionName, Function function) {
		if (functionName.equalsIgnoreCase(MongoDBExecutionFactory.FUNC_GEO_NEAR) || 
				functionName.equalsIgnoreCase(MongoDBExecutionFactory.FUNC_GEO_NEAR_SPHERE)) {
			return buildGeoNearFunction(function);
		}
		return buildGeoFunction(function);			
	}

	private DBObject buildGeoNearFunction(Function function) {
		List<Expression> args = function.getParameters();

		// Column Name		
		int paramIndex = 0;
		ColumnDetail column = getExpressionAlias(args.get(paramIndex++));

		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
		builder.push(column.documentQueryFieldName);
		builder.push(function.getName());						
		builder.push("$geometry");//$NON-NLS-1$
		builder.add("type", SpatialType.Point.name());//$NON-NLS-1$
		
		// walk the co-ordinates
		append(args.get(paramIndex++));
		BasicDBList coordinates = new BasicDBList();
		coordinates.add(this.onGoingExpression.pop());
		builder.add("coordinates", coordinates); //$NON-NLS-1$
		
		// maxdistance
		append(args.get(paramIndex++));
		builder.pop().add("$maxDistance", this.onGoingExpression.pop()); //$NON-NLS-1$
		
		return builder.get();
	}

	private DBObject buildGeoFunction(Function function) {
		List<Expression> args = function.getParameters();

		// Column Name		
		int paramIndex = 0;
		ColumnDetail column = getExpressionAlias(args.get(paramIndex++));

		// Type: Point, LineString, Polygon..
		append(args.get(paramIndex++));
		SpatialType type = SpatialType.valueOf((String)this.onGoingExpression.pop());
		
		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
		builder.push(column.documentQueryFieldName);
		builder.push(function.getName());			
		builder.push("$geometry");//$NON-NLS-1$
		builder.add("type", type.name());//$NON-NLS-1$
		
		// walk the co-ordinates
		append(args.get(paramIndex++));
		BasicDBList coordinates = new BasicDBList();
		coordinates.add(this.onGoingExpression.pop());
		builder.add("coordinates", coordinates); //$NON-NLS-1$
		return builder.get();	
	}	
}
