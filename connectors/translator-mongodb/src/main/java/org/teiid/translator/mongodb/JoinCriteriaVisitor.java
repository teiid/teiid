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

import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Join;
import org.teiid.language.Join.JoinType;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MutableDBRef.Association;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

class JoinCriteriaVisitor extends HierarchyVisitor {
	private Join join;
	private MongoDocument left; 
	private MongoDocument right;
	//protected Stack<String> onGoingExpression  = new Stack<String>();
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private DBObject match =  null;
	private BasicDBObject projection =  null;
	private String aliasName;
	
	public JoinCriteriaVisitor(Join join, MongoDocument left, MongoDocument right) {
		this.join = join;
		this.left = left;
		this.right = right;
		
		if (join.getCondition() != null) {
			visitNode(join.getCondition());
		}
	}
	
	@Override
	public void visit(ColumnReference obj) {
		/*
		String columnName = obj.getMetadataObject().getName();
		Table table = (Table)obj.getMetadataObject().getParent();
		
		String selectionName = null;
		if (MongoDBSelectVisitor.isPartOfPrimaryKey(table, columnName)) {
			//TODO:should deal with composite key later
			selectionName = "_id"; //$NON-NLS-1$
		}
		else {
			selectionName = columnName;
		}
		
		try {
			if (this.left.embeds(this.right)) {
				if (table.equals(this.right.getTable())) {
					selectionName = table.getName()+"."+selectionName; //$NON-NLS-1$
				}
			}
			else {
				// right embeds left
				if (table.equals(left.getTable())) {
					selectionName = table.getName()+"."+selectionName; //$NON-NLS-1$
				}
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
		this.onGoingExpression.push(selectionName);
		*/
	}
	
	@Override
	public void visit(Comparison obj) {
        /*
		visitNode(obj.getLeftExpression());
		String leftExpr = this.onGoingExpression.pop();
		
		visitNode(obj.getRightExpression());
		String rightExpr = this.onGoingExpression.pop();
		*/
		
		DBObject match =  null;
		try {
			// left outer join we do not need to any thing, if left is the parent, if right is parent then it 
			// it is similar to right outer join which is not supported
			// inner join needs "exists" on embedded doc
			switch(obj.getOperator()) {
			case EQ:
				if (join.getJoinType().equals(JoinType.LEFT_OUTER_JOIN)) {
					if (left.contains(right)) {
						// if nesting is simple flat hierary then there is nothing to be done. However if
						// document is array is $unwind behavior is strange, it does not include the document
						// that has empty or null child document. So, we need to simulate such that there is 
						// some nested doc using "$ifnull"
						if (this.right.isMerged() && this.right.getMergeAssociation().equals(Association.MANY)) {
							this.projection = buildIfNullBasedProjection(this.left, this.right);
						}
					}
					else {
						// right is parent; left is child. However, left does not exist with out its parent
						// so in "MERGE" scenario, this is equal to a INNER JOIN
						if (left.isMerged()) {
							match = QueryBuilder.start(left.getTable().getName()).exists("true").notEquals(null).get(); //$NON-NLS-1$							
						}
						else {
							//so, right is parent, now this is un-supported
							throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18022, right.getTable().getName(), left.getTable().getName()));
						}
					}
				}
				else if (join.getJoinType().equals(JoinType.INNER_JOIN)){
					if (left.contains(right)) {
						match = QueryBuilder.start(right.getTable().getName()).exists("true").notEquals(null).get(); //$NON-NLS-1$
					}
					else {
						match = QueryBuilder.start(left.getTable().getName()).exists("true").notEquals(null).get(); //$NON-NLS-1$ 				
					}
				}
				else if (join.getJoinType().equals(JoinType.CROSS_JOIN) || join.getJoinType().equals(JoinType.FULL_OUTER_JOIN)){
					throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18022, left.getTable().getName(), right.getTable().getName(), join.getJoinType()));			
				}        	
				break;
			case NE:
			case LT:
			case LE:
			case GT:
			case GE:
				this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18023)));
				break;
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
    	
    	this.match = match;
	}

	DBObject getCondition() throws TranslatorException {
		if (!this.exceptions.isEmpty()) {
			throw exceptions.get(0);
		}
		return this.match;
	}

	BasicDBObject getProjection() throws TranslatorException {
		if (!this.exceptions.isEmpty()) {
			throw exceptions.get(0);
		}
		return this.projection;
	}	
	
	String getAliasName() {
		return this.aliasName;
	}
	
	private BasicDBObject buildIfNullBasedProjection(MongoDocument parent, MongoDocument child) {
		
		BasicDBObject columns = new BasicDBObject();
		Table table = parent.getTable();
		for (Column c:table.getColumns()) {
			columns.append(c.getName(), 1);
		}

		BasicDBList exprs = new BasicDBList();
		exprs.add("$"+child.getTable().getName()); //$NON-NLS-1$
		BasicDBList list = new BasicDBList();
		list.add(new BasicDBObject());
		exprs.add(list); 
		BasicDBObject ifnull = new BasicDBObject("$ifNull", exprs); //$NON-NLS-1$
		this.aliasName = "__NN_"+child.getTable().getName();//$NON-NLS-1$
		columns.append(this.aliasName, ifnull); 
		child.getMergeKey().setAlias(this.aliasName);
		
		return columns;
	}
}
