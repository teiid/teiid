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

import org.teiid.language.ColumnReference;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MutableDBRef.Assosiation;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

public class MongoDBUpdateVisitor extends MongoDBSelectVisitor {

	protected LinkedHashMap<String, Object> columnValues = new LinkedHashMap<String, Object>();

	public MongoDBUpdateVisitor(MongoDBExecutionFactory executionFactory, RuntimeMetadata metadata) {
		super(executionFactory, metadata);
	}

	@Override
	public void visit(Insert obj) {
        append(obj.getTable());

        this.columnValues.putAll(this.foreignKeys);

        List<ColumnReference> columns = obj.getColumns();
        List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();

		try {
			for (int i = 0; i < columns.size(); i++) {
				String colName = getColumnName(columns.get(i));
				Expression expr = values.get(i);
				resolveExpressionValue(colName, expr);
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}
	}

	private void resolveExpressionValue(String colName, Expression expr) throws TranslatorException {
		Object value = null;
		if (expr instanceof Literal) {
			value = this.executionFactory.convertToMongoType(((Literal) expr).getValue());
		}
		else {
			this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18001)));
		}

		if (this.columnValues.get(colName) != null) {
			((MutableDBRef) this.columnValues.get(colName)).setId(value);

			// parent table selection query.
			if (this.pushKey != null && this.pushKey.getReferenceColumnName().equals(colName)) {
				this.pushKey.setId(value);
			}

			// child table selection query
			if (!this.pullKeys.isEmpty()) {
				for (MutableDBRef ref:this.pullKeys) {
					if (ref.getColumnName().equals(colName)) {
						ref.setId(value);
					}
				}
			}
		}
		else {
			this.columnValues.put(colName, value);
		}
	}

	@Override
	public void visit(Update obj) {
        append(obj.getTable());

        this.columnValues.putAll(this.foreignKeys);

        List<SetClause> changes = obj.getChanges();
        try {
			for (SetClause clause:changes) {
				String colName = getColumnName(clause.getSymbol());
				Expression expr = clause.getValue();
				resolveExpressionValue(colName, expr);
			}
		} catch (TranslatorException e) {
			this.exceptions.add(e);
		}

        append(obj.getWhere());

        if (!this.onGoingCriteria.isEmpty()) {
        	this.match = this.onGoingCriteria.pop();
        }
	}

	@Override
	public void visit(Delete obj) {
		append(obj.getTable());
        append(obj.getWhere());

        if (!this.onGoingCriteria.isEmpty()) {
        	this.match = this.onGoingCriteria.pop();
        }
	}

	public BasicDBObject getInsert(DB db, LinkedHashMap<String, DBObject> embeddedDocuments) throws TranslatorException {
		BasicDBObject insert = new BasicDBObject();
		for (String key:this.columnValues.keySet()) {
			Object obj = this.columnValues.get(key);
			if (obj instanceof MutableDBRef) {
				insert.append(key, ((MutableDBRef)obj).getDBRef(db, true));
			}
			else {
				insert.append(key, obj);
			}
		}

		if (this.pullKeys != null) {
			for (MutableDBRef ref: this.pullKeys) {
				DBObject embedDoc = embeddedDocuments.get(ref.getEmbeddedTable());
				if (embedDoc == null) {
					throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18008, ref.getEmbeddedTable()));
				}
				insert.append(ref.getEmbeddedTable(), embedDoc);
			}
		}
		return insert;
	}

	public BasicDBObject getUpdate(DB db, LinkedHashMap<String, DBObject> embeddedDocuments) throws TranslatorException {
		BasicDBObject update = new BasicDBObject();

		String embeddedDocumentName = null;
		if (this.pushKey != null) {
			embeddedDocumentName = this.pushKey.getEmbeddedTable();
		}

		for (String key:this.columnValues.keySet()) {
			Object obj = this.columnValues.get(key);
			if (obj instanceof MutableDBRef) {
				MutableDBRef ref = ((MutableDBRef)obj);

				if (ref.getId() != null) {
					if (this.pushKey != null) {
						// do not allow updating the main document reference where this embedded document is embedded.
						if (ref.getParentTable().equals(this.pushKey.getParentTable())) {
							throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18007, ref.getParentTable(), embeddedDocumentName));
						}
					}

					update.append(key, ref.getDBRef(db, true));

					// also update the embedded document
					if (this.pullKeys != null) {
						for (MutableDBRef pullRef: this.pullKeys) {
							if (ref.getParentTable().equals(pullRef.getEmbeddedTable())) {
								DBObject embedDoc = embeddedDocuments.get(pullRef.getEmbeddedTable());
								if (embedDoc == null) {
									throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18008, ref.getEmbeddedTable()));
								}
								update.append(pullRef.getEmbeddedTable(), embedDoc);
							}
						}
					}
				}
			}
			else {
				if (this.pushKey != null && this.pushKey.getAssosiation() == Assosiation.MANY) {
					update.append(embeddedDocumentName+".$."+key, obj); //$NON-NLS-1$
				}
				else {
					update.append(((embeddedDocumentName == null)?key:embeddedDocumentName+"."+key), obj); //$NON-NLS-1$
				}
			}
		}
		return update;
	}
}
