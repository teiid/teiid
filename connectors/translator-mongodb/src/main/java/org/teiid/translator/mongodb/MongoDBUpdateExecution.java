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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.teiid.GeneratedKeys;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.mongodb.MutableDBRef.Assosiation;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class MongoDBUpdateExecution extends MongoDBBaseExecution implements UpdateExecution {
	private Command command;
	private MongoDBUpdateVisitor visitor;
	private MongoDBExecutionFactory executionFactory;
	private int[] results;

	public MongoDBUpdateExecution(MongoDBExecutionFactory executionFactory,
			Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			MongoDBConnection connection) throws TranslatorException {
		super(executionContext, metadata, connection);
		this.command = command;

		this.visitor = new MongoDBUpdateVisitor(executionFactory, metadata, this.mongoDB);
		this.visitor.visitNode(command);

		if (!this.visitor.exceptions.isEmpty()) {
			throw this.visitor.exceptions.get(0);
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}

	@Override
	public void execute() throws TranslatorException {
		DBCollection collection = getCollection(this.visitor.collectionTable.getName());

		WriteResult result = null;
		if (this.command instanceof Insert) {
			// get pull key based documents to embed
			LinkedHashMap<String, DBObject> embeddedDocuments = fetchEmbeddedDocuments();

			// check if this document need to be embedded in any other document
			if (this.visitor.pushKey != null) {
				DBRef ref = this.visitor.pushKey.getDBRef(this.mongoDB, true);
				DBObject match = ref.fetch();
				if (match == null) {
					throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18006, this.visitor.pushKey.getParentTable(), this.visitor.pushKey.getId(), this.visitor.pushKey.getEmbeddedTable()));
				}

				BasicDBObject embeddedDoc = new BasicDBObject(this.visitor.pushKey.getEmbeddedTable(), this.visitor.getInsert(this.mongoDB, embeddedDocuments));
				if (this.visitor.pushKey.getAssosiation() == Assosiation.MANY) {
					result = collection.update(match, new BasicDBObject("$push", embeddedDoc), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
				}
				else {
					result = collection.update(match, new BasicDBObject("$set", embeddedDoc), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
				}
			}
			else {
				// gets its own collection
				result = collection.insert(this.visitor.getInsert(this.mongoDB, embeddedDocuments), WriteConcern.ACKNOWLEDGED);
			}
		}
		else if (this.command instanceof Update) {
			// get pull key based documents to embed
			LinkedHashMap<String, DBObject> embeddedDocuments = fetchEmbeddedDocuments();
			DBObject match = new BasicDBObject();
			if (this.visitor.match != null) {
				match = this.visitor.match;
			}

			result = collection.update(match, new BasicDBObject("$set", this.visitor.getUpdate(this.mongoDB, embeddedDocuments)), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$

			// if the update is for the "embeddable" table, then since it is copied to other tables
			// those references need to be updated. I know this is not atomic operation, but not sure
			// how else to handle it.
			if (!this.visitor.tableCopiedIn.isEmpty()) {
				if (result.getError() == null) {
					AggregationOutput output = collection.aggregate(new BasicDBObject("$match", match)); //$NON-NLS-1$
					Iterator<DBObject> resultset = output.results().iterator();
					while(resultset.hasNext()) {
						DBObject row = resultset.next();
						if (row != null) {
							for (MutableDBRef ref:this.visitor.tableCopiedIn) {
								DBCollection parent = getCollection(ref.getParentTable());
								WriteResult update = parent.update(new BasicDBObject(ref.getRefName()+".$id", row.get("_id")), new BasicDBObject("$set",new BasicDBObject(ref.getEmbeddedTable(), row)), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
								if (update.getError() != null) {
									throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18009));
								}
							}
						}
					}
				}
			}
		}
		else {
			DBObject match = new BasicDBObject();
			if (this.visitor.match != null) {
				match = this.visitor.match;
			}

			if (!this.visitor.tableCopiedIn.isEmpty()) {
				AggregationOutput output = collection.aggregate(new BasicDBObject("$match", match)); //$NON-NLS-1$
				Iterator<DBObject> resultset = output.results().iterator();
				while(resultset.hasNext()) {
					DBObject row = resultset.next();
					if (row != null) {
						for (MutableDBRef ref:this.visitor.tableCopiedIn) {
							DBCollection parent = getCollection(ref.getParentTable());
							AggregationOutput referenceOutput = parent.aggregate(new BasicDBObject("$match", new BasicDBObject(ref.getRefName()+".$id", row.get("_id")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							if (referenceOutput.results().iterator().hasNext()) {
								throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18010, this.visitor.collectionTable.getName(), ref.getParentTable()));
							}
						}
					}
				}
			}

			result = collection.remove(match, WriteConcern.ACKNOWLEDGED);
		}

		if (result != null) {
			if (result.getError() != null) {
				throw new TranslatorException(result.getError());
			}
			this.results = new int[1];
			this.results[0] = result.getN();

			if (this.command instanceof Insert) {
	            if (this.executionContext.getCommandContext().isReturnAutoGeneratedKeys()) {
	            	addAutoGeneretedKeys(result);
	            }
			}
		}
	}

	private LinkedHashMap<String, DBObject> fetchEmbeddedDocuments() {
		LinkedHashMap<String, DBObject> additionalDocuments = new LinkedHashMap<String, DBObject>();

		// check if there are any other documents that can be embedded in this
		// document
		if (!this.visitor.pullKeys.isEmpty()) {
			for (MutableDBRef ref:this.visitor.pullKeys) {
				DBRef dbRef = ref.getDBRef(this.mongoDB, false);
				DBObject document = dbRef.fetch();
				if (document == null) {
					continue;
				}
				additionalDocuments.put(ref.getEmbeddedTable(), document);
			}
		}
		return additionalDocuments;
	}

	private DBCollection getCollection(String name) {
		DBCollection collection;
		if (!this.mongoDB.collectionExists(name)) {
			collection = this.mongoDB.createCollection(name, null);

			// since this is the first time creating the tables; create the indexes on the collection
			Table table = this.visitor.collectionTable;
//			if (table.getPrimaryKey() != null) {
//				createIndex(collection, table.getPrimaryKey(), true);
//			}

			// index on foreign keys
			for (ForeignKey record:table.getForeignKeys()) {
				createIndex(collection, record, false);
			}

			// index on unique
			for (KeyRecord record:table.getUniqueKeys()) {
				createIndex(collection, record, true);
			}

			// index on index keys
			for (KeyRecord record:table.getIndexes()) {
				createIndex(collection, record, false);
			}
		}
		else {
			collection = this.mongoDB.getCollection(name);
		}
		return collection;
	}

	private void createIndex(DBCollection collection, KeyRecord record, boolean unique) {
		BasicDBObject key = new BasicDBObject();
		for (Column c:record.getColumns()) {
			key.append(MongoDBSelectVisitor.getRecordName(c), 1);
		}
		collection.ensureIndex(key, record.getName(), unique);
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return this.results;
	}


	private void addAutoGeneretedKeys(WriteResult result) {
		Table table = this.visitor.collectionTable;

		int cols = table.getPrimaryKey().getColumns().size();
		Class<?>[] columnDataTypes = new Class<?>[cols];
		String[] columnNames = new String[cols];
		//this is typically expected to be an int/long, but we'll be general here.  we may eventual need the type logic off of the metadata importer
        for (int i = 0; i < cols; i++) {
        	columnDataTypes[i] = table.getPrimaryKey().getColumns().get(i).getJavaType();
        	columnNames[i] = table.getPrimaryKey().getColumns().get(i).getName();
        }
        GeneratedKeys generatedKeys = this.executionContext.getCommandContext().returnGeneratedKeys(columnNames, columnDataTypes);
        List<Object> vals = new ArrayList<Object>(columnDataTypes.length);
        for (int i = 0; i < columnDataTypes.length; i++) {
            Object value = this.executionFactory.retrieveValue(result.getField(columnNames[i]), columnDataTypes[i], this.mongoDB, columnNames[i], columnNames[i]);
            vals.add(value);
        }
        generatedKeys.addKey(vals);
	}
}
