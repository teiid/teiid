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

import java.util.LinkedHashMap;
import java.util.List;

import org.teiid.language.*;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MutableDBRef.Association;

import com.mongodb.*;

public class MongoDBUpdateVisitor extends MongoDBSelectVisitor {

	protected LinkedHashMap<String, Object> columnValues = new LinkedHashMap<String, Object>();
	private DB mongoDB;
	private BasicDBObject pull;
	private Condition condition;

	public MongoDBUpdateVisitor(MongoDBExecutionFactory executionFactory, RuntimeMetadata metadata, DB mongoDB) {
		super(executionFactory, metadata);
		this.mongoDB = mongoDB;
	}

	@Override
	public void visit(Insert obj) {
        append(obj.getTable());

        List<ColumnReference> columns = obj.getColumns();
        List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();

		try {
			for (int i = 0; i < columns.size(); i++) {
				String colName = getColumnName(columns.get(i));
				Expression expr = values.get(i);
				resolveExpressionValue(obj.getTable().getName(), colName, expr);
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}

	private void resolveExpressionValue(String tableName, String colName, Expression expr) throws TranslatorException {
		Object value = null;
		if (expr instanceof Literal) {
			value = this.executionFactory.convertToMongoType(((Literal) expr).getValue(), this.mongoDB, colName);
		}
		else if (expr instanceof org.teiid.language.Array) {
		    org.teiid.language.Array contents = (org.teiid.language.Array)expr;
		    List<Expression> arrayExprs = contents.getExpressions();
		    value = new BasicDBList();
		    for (Expression exp:arrayExprs) {
		        if (exp instanceof Literal) {
		            ((BasicDBList)value).add(this.executionFactory.convertToMongoType(((Literal) exp).getValue(), this.mongoDB, colName));
		        }
		        else {
		            this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18001)));
		        }
		    }
		}
		else {
			this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18001)));
		}

		this.columnValues.put(colName, value);

		// Update he mongo document to keep track the reference values.
		this.mongoDoc.updateReferenceColumnValue(tableName, colName, value);

		// if this FK column, replace with reference rather than simple key value
		if (this.mongoDoc.isPartOfForeignKey(colName)) {
			this.columnValues.put(colName, this.mongoDoc.getFKReference(colName));
		}
	}

	@Override
	public void visit(Update obj) {
		this.condition = obj.getWhere();
        append(obj.getTable());

        List<SetClause> changes = obj.getChanges();
        try {
			for (SetClause clause:changes) {
				String colName = getColumnName(clause.getSymbol());
				Expression expr = clause.getValue();
				resolveExpressionValue(obj.getTable().getName(), colName, expr);
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}

        append(obj.getWhere());

        if (!this.onGoingExpression.isEmpty()) {
        	this.match = (DBObject)this.onGoingExpression.pop();
        }
	}

	@Override
	public void visit(Delete obj) {
		this.condition = obj.getWhere();
		append(obj.getTable());
        append(obj.getWhere());

        if (!this.onGoingExpression.isEmpty()) {
        	this.match = (DBObject)this.onGoingExpression.pop();
        }
	}

	public BasicDBObject getInsert(DB db, LinkedHashMap<String, DBObject> embeddedDocuments) {
		IDRef pk = null;

		BasicDBObject insert = new BasicDBObject();
		for (String key:this.columnValues.keySet()) {
			Object obj = this.columnValues.get(key);

			if (obj instanceof MutableDBRef) {
				obj =  ((MutableDBRef)obj).getDBRef(db, true);
			}

			if (this.mongoDoc.isPartOfPrimaryKey(key)) {
				if (pk == null) {
					pk = new IDRef();
				}
				pk.addColumn(key, obj);
			}
			else {
				insert.append(key, obj);
			}
		}

		if (pk != null) {
			insert.append("_id", pk.getValue()); //$NON-NLS-1$
		}

		if (this.mongoDoc.hasEmbeddedDocuments()) {
			for (String docName:this.mongoDoc.getEmbeddedDocumentNames()) {
				DBObject embedDoc = embeddedDocuments.get(docName);
				if (embedDoc != null) {
					insert.append(docName, embedDoc);
				}
			}
		}
		return insert;
	}

	public BasicDBObject getUpdate(DB db, LinkedHashMap<String, DBObject> embeddedDocuments) throws TranslatorException {
		BasicDBObject update = new BasicDBObject();

		String embeddedDocumentName = null;
		if (this.mongoDoc.isMerged()) {
			embeddedDocumentName = this.mongoDoc.getTable().getName();
		}

		for (String key:this.columnValues.keySet()) {
			Object obj = this.columnValues.get(key);

			if (obj instanceof MutableDBRef) {
				MutableDBRef ref = ((MutableDBRef)obj);

				if (this.mongoDoc.isMerged()) {
					// do not allow updating the main document reference where this embedded document is embedded.
					if (ref.getParentTable().equals(this.mongoDoc.getMergeTable().getName())) {
						throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18007, ref.getParentTable(), embeddedDocumentName));
					}
				}

				update.append(key, ref.getDBRef(db, true));

				// also update the embedded document
				if (this.mongoDoc.hasEmbeddedDocuments()) {
					for (MutableDBRef docKey: this.mongoDoc.getEmbeddableReferences()) {
						if (ref.getParentTable().equals(docKey.getEmbeddedTable())) {
							DBObject embedDoc = embeddedDocuments.get(docKey.getName());
							if (embedDoc != null) {
								update.append(docKey.getName(), embedDoc);
							}
							else {
								update.append(docKey.getName(), null);
							}
						}
					}
				}
			}
			else {
				if (this.mongoDoc.isMerged()) {
					if (this.mongoDoc.getMergeAssociation() == Association.MANY) {
						update.append(embeddedDocumentName+".$."+key, obj); //$NON-NLS-1$
					}
					else {
						update.append(embeddedDocumentName+"."+key, obj); //$NON-NLS-1$
					}
				}
				else {
					if (isPartOfPrimaryKey(this.mongoDoc.getTargetTable(), key)) {
						if (hasCompositePrimaryKey(this.mongoDoc.getTargetTable())) {
							update.append("_id."+key, obj);//$NON-NLS-1$
						}
						else {
							update.append("_id", obj); //$NON-NLS-1$
						}
					}
					else {
						update.append(key, obj);
					}
				}
			}
		}
		return update;
	}

	public DBObject getPullQuery() {
		if (this.match == null) {
			return QueryBuilder.start(mongoDoc.getTable().getName()).exists("true").get(); //$NON-NLS-1$
		}
		if (this.pull == null) {
			this.pull =  new BasicDBObject(this.mongoDoc.getTable().getName(), this.onGoingPullCriteria.pop());
		}
		return this.pull;

	}

	public BasicDBList updateMerge(DB db, BasicDBList previousRows) throws TranslatorException {
		BasicDBList updated = new BasicDBList();

		for (int i = 0; i < previousRows.size(); i++) {
			BasicDBObject row = (BasicDBObject)previousRows.get(i);
			if (this.match == null || ExpressionEvaluator.matches(this.condition, row)) {
				for (String key:this.columnValues.keySet()) {
					Object obj = this.columnValues.get(key);

					if (obj instanceof MutableDBRef) {
						MutableDBRef ref = ((MutableDBRef)obj);
						row.put(key, ref.getDBRef(db, true));
					}
					else {
						row.put(key, obj);
					}
				}
				updated.add(row);
			}
		}

		return updated;
	}
	
	public BasicDBObject updateMerge(DB db, BasicDBObject previousRow) throws TranslatorException {
		if (this.match == null || ExpressionEvaluator.matches(this.condition, previousRow)) {
			for (String key:this.columnValues.keySet()) {
				Object obj = this.columnValues.get(key);
	
				if (obj instanceof MutableDBRef) {
					MutableDBRef ref = ((MutableDBRef)obj);
					previousRow.put(key, ref.getDBRef(db, true));
				}
				else {
					previousRow.put(key, obj);
				}
			}
		}
		return previousRow;
	}	

}
