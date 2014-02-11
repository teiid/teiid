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
import org.teiid.translator.mongodb.MongoDocument.MergeDetails;
import org.teiid.translator.mongodb.MutableDBRef.Association;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
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
		DBCollection collection = getCollection(this.visitor.mongoDoc.getTargetTable());
		MongoDocument mongoDoc = this.visitor.mongoDoc;

		WriteResult result = null;
		if (this.command instanceof Insert) {
			// get pull key based documents to embed
			LinkedHashMap<String, DBObject> embeddedDocuments = fetchEmbeddedDocuments();

			// check if this document need to be embedded in any other document
			if (mongoDoc.isMerged()) {
				MergeDetails mergeInfo = mongoDoc.getMergeParentCriteria(this.mongoDB, null, null, this.visitor.getInsert(this.mongoDB, embeddedDocuments), false);

				if (mergeInfo.association.equals(Association.MANY)) {
					result = collection.update(mergeInfo.match, new BasicDBObject("$push", mergeInfo.update), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
				}
				else {
					result = collection.update(mergeInfo.match, new BasicDBObject("$set", mergeInfo.update), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
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

			if (mongoDoc.isMerged()) {
				MergeDetails mergeInfo = mongoDoc.getMergeParentCriteria(this.mongoDB, null, null, this.visitor.getUpdate(this.mongoDB, embeddedDocuments), false);
				if (mergeInfo.nested) {
					throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18016));
				}
				// multi items in array update not available, http://jira.mongodb.org/browse/SERVER-1243
				// this work-around for above issue, re-writes whole array but atomic.
				Iterator<DBObject> output = collection.aggregate(new BasicDBObject("$match", match)).results().iterator(); //$NON-NLS-1$
				while(output.hasNext()) {
					DBObject row = output.next();
					if (mergeInfo.association.equals(Association.MANY)) {
						BasicDBList previousMerge = (BasicDBList)row.get(mongoDoc.getTable().getName());
						BasicDBList updatedDoc = this.visitor.updateMerge(this.mongoDB, previousMerge);
						if (updatedDoc.size() != 0) {
							result = collection.update(new BasicDBObject("_id", row.get("_id")), new BasicDBObject("$set", new BasicDBObject(mongoDoc.getTable().getName(), updatedDoc)), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
						}
					}
					else {
						BasicDBObject previousMerge = (BasicDBObject)row.get(mongoDoc.getTable().getName());
						if (previousMerge != null) {
							BasicDBObject updatedDoc = this.visitor.updateMerge(this.mongoDB, previousMerge);
							result = collection.update(new BasicDBObject("_id", row.get("_id")), new BasicDBObject("$set", new BasicDBObject(mongoDoc.getTable().getName(), updatedDoc)), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
						}
					}
				}
				//result = collection.update(match, new BasicDBObject("$set", this.visitor.getUpdate(this.mongoDB, embeddedDocuments)), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
			}
			else {
				result = collection.update(match, new BasicDBObject("$set", this.visitor.getUpdate(this.mongoDB, embeddedDocuments)), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
			}

			// if the update is for the "embeddable" table, then since it is copied to other tables
			// those references need to be updated. I know this is not atomic operation, but not sure
			// how else to handle it.
			if (mongoDoc.isEmbeddable()) {
				if (result.getError() == null) {
					updateReferenceTables(collection, mongoDoc, match);
				}
			}
		}
		else {
			DBObject match = new BasicDBObject();
			if (this.visitor.match != null) {
				match = this.visitor.match;
			}

			if (mongoDoc.isEmbeddable()) {
				AggregationOutput output = collection.aggregate(new BasicDBObject("$match", match)); //$NON-NLS-1$
				Iterator<DBObject> resultset = output.results().iterator();
				while(resultset.hasNext()) {
					DBObject row = resultset.next();
					if (row != null) {
						for (MutableDBRef ref:mongoDoc.getEmbeddedInReferences()) {
							DBCollection parent = getCollection(ref.getParentTable());
							AggregationOutput referenceOutput = parent.aggregate(new BasicDBObject("$match", new BasicDBObject(ref.getReferenceName()+".$id", row.get("_id")))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
							if (referenceOutput.results().iterator().hasNext()) {
								throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18010, this.visitor.mongoDoc.getTargetTable().getName(), ref.getParentTable()));
							}
						}
					}
				}
			}

			if (mongoDoc.isMerged()) {
				MergeDetails mergeInfo = mongoDoc.getMergeParentCriteria(this.mongoDB, this.visitor.match, null, new BasicDBObject(), false);
				if (mergeInfo.nested) {
					throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18016));
				}
				if (mergeInfo.association.equals(Association.MANY)) { 
					result = collection.update(mergeInfo.match, new BasicDBObject("$pull", this.visitor.getPullQuery()), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
				}
				else {
					result = collection.update(mergeInfo.match, new BasicDBObject("$unset", new BasicDBObject(mongoDoc.getTable().getName(), "")), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$ //$NON-NLS-2$					
				}
			}
			else {
				result = collection.remove(match, WriteConcern.ACKNOWLEDGED);
			}
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

	private void updateReferenceTables(DBCollection collection, MongoDocument mongoDoc, DBObject match) throws TranslatorException {
		AggregationOutput output = collection.aggregate(new BasicDBObject("$match", match)); //$NON-NLS-1$
		Iterator<DBObject> resultset = output.results().iterator();
		while(resultset.hasNext()) {
			DBObject row = resultset.next();
			if (row != null) {
				for (MutableDBRef ref:mongoDoc.getEmbeddedInReferences()) {
					DBCollection parent = getCollection(ref.getParentTable());
					DBObject parentmatch = new BasicDBObject(ref.getReferenceName()+".$id", row.get("_id")); //$NON-NLS-1$ //$NON-NLS-2$
					WriteResult update = parent.update(parentmatch, new BasicDBObject("$set",new BasicDBObject(ref.getName(), row)), //$NON-NLS-1$
							false, true, WriteConcern.ACKNOWLEDGED);

					if (update.getError() != null) {
						throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18009));
					}

					// see if there are nested references
					Table parentTable = this.metadata.getTable(mongoDoc.getTable().getParent().getName(), ref.getParentTable());
					MongoDocument parentMongoDocument = new MongoDocument(parentTable, this.metadata);
					if (parentMongoDocument.isEmbeddable()) {
						updateReferenceTables(parent, parentMongoDocument, parentmatch);
					}
				}
			}
		}
	}

	private LinkedHashMap<String, DBObject> fetchEmbeddedDocuments() {
		LinkedHashMap<String, DBObject> additionalDocuments = new LinkedHashMap<String, DBObject>();

		// check if there are any other documents that can be embedded in this
		// document
		MongoDocument mongoDoc = this.visitor.mongoDoc;
		if (mongoDoc.hasEmbeddedDocuments()) {
			for (String docName:mongoDoc.getEmbeddedDocumentNames()) {
				DBObject document = mongoDoc.getEmbeddedDocument(this.mongoDB, docName);
				if (document == null) {
					continue;
				}
				additionalDocuments.put(docName, document);
			}
		}
		return additionalDocuments;
	}

	private DBCollection getCollection(String name) throws TranslatorException {
		return getCollection(this.metadata.getTable(this.visitor.mongoDoc.getTable().getParent().getName(), name));
	}
	private DBCollection getCollection(Table table) {
		DBCollection collection;
		if (!this.mongoDB.collectionExists(table.getName())) {
			collection = this.mongoDB.createCollection(table.getName(), null);

			// since this is the first time creating the tables; create the indexes on the collection
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
			collection = this.mongoDB.getCollection(table.getName());
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


	private void addAutoGeneretedKeys(WriteResult result) throws TranslatorException {
		Table table = this.visitor.mongoDoc.getTargetTable();

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
