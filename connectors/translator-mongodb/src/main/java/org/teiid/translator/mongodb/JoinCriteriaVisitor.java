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

import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Join.JoinType;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MergeDetails.Association;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

class JoinCriteriaVisitor extends HierarchyVisitor {
	private JoinType joinType;
	private MongoDocument left;
	private MongoDocument right;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	protected MergePlanner mergePlanner;
	private String aliasName;

	public JoinCriteriaVisitor(JoinType joinType, MongoDocument left, MongoDocument right, MergePlanner mergePlanner) {
		this.joinType = joinType;
		this.left = left;
		this.right = right;
		this.mergePlanner = mergePlanner;
	}

	public void process(Condition condition) throws TranslatorException {
        if (condition != null) {
            visitNode(condition);
        }

        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
	}

	@Override
	public void visit(Comparison obj) {

		try {
			// left outer join we do not need to any thing, if left is the parent, if right is parent then it
			// it is similar to right outer join which is not supported
			// inner join needs "exists" on embedded doc
			switch(obj.getOperator()) {
			case EQ:
				if (this.joinType.equals(JoinType.LEFT_OUTER_JOIN)) {
					if (left.contains(right)) {
						// if nesting is simple flat hierary then there is nothing to be done. However if
						// document is array is $unwind behavior is strange, it does not include the document
						// that has empty or null child document. So, we need to simulate such that there is
						// some nested doc using "$ifnull"
						if (this.right.isMerged() && this.right.getMergeAssociation().equals(Association.MANY)) {
							this.mergePlanner.addNode(new ProjectionNode(this.left, buildIfNullBasedProjection(this.left, this.right)), this.aliasName);
						}
					}
					else {
						// right is parent; left is child. However, left does not exist with out its parent
						// so in "MERGE" scenario, this is equal to a INNER JOIN
						if (left.isMerged()) {
						    this.mergePlanner.addNode(new ExistsNode(this.left));
						}
						else {
							//so, right is parent, now this is un-supported
							throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18022, right.getTable().getName(), left.getTable().getName()));
						}
					}
				}
				else if (this.joinType.equals(JoinType.INNER_JOIN)){
					if (this.left.contains(this.right)) {
					    this.mergePlanner.addNode(new ExistsNode(this.right));
					}
					else {
					    this.mergePlanner.addNode(new ExistsNode(this.left));
					}
				}
				else if (this.joinType.equals(JoinType.CROSS_JOIN) || this.joinType.equals(JoinType.FULL_OUTER_JOIN)){
					throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18022, left.getTable().getName(), right.getTable().getName(), this.joinType));
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
	}

	private BasicDBObject buildIfNullBasedProjection(MongoDocument parent, MongoDocument child) throws TranslatorException {
		BasicDBObject columns = new BasicDBObject();
		Table table = parent.getTable();
		for (Column c:table.getColumns()) {
		    if (parent.isMerged() || parent.isEmbeddable()) {
		        columns.append(parent.getQualifiedName(false)+"."+c.getName(), 1); //$NON-NLS-1$
		    }
		    else {
		        columns.append(c.getName(), 1);
		    }
		}

		BasicDBList exprs = new BasicDBList();
		exprs.add("$"+child.getQualifiedName(false)); //$NON-NLS-1$
		BasicDBList list = new BasicDBList();
		list.add(new BasicDBObject());
		exprs.add(list);
		BasicDBObject ifnull = new BasicDBObject("$ifNull", exprs); //$NON-NLS-1$
		this.aliasName = "__NN_"+child.getTable().getName();//$NON-NLS-1$
		columns.append(this.aliasName, ifnull);
		child.setAlias(this.aliasName);
		child.getMergeKey().setAlias(this.aliasName);

		return columns;
	}
}
