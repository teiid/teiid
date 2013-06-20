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
import java.util.List;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDBQueryExecution extends MongoDBBaseExecution implements ResultSetExecution {
	private Select command;
	private MongoDBExecutionFactory executionFactory;
	private Iterator<DBObject> results;
	private MongoDBSelectVisitor visitor;
	private Class<?>[] expectedTypes;

	public MongoDBQueryExecution(
			MongoDBExecutionFactory executionFactory,
			QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, MongoDBConnection connection) {
		super(executionContext, metadata, connection);
		this.command = (Select)command;
		this.executionFactory = executionFactory;
		this.expectedTypes = command.getColumnTypes();

	}

	@Override
	public void execute() throws TranslatorException {
		this.visitor = new MongoDBSelectVisitor(this.executionFactory, this.metadata);
		this.visitor.visitNode(this.command);

		if (!this.visitor.exceptions.isEmpty()) {
    		throw this.visitor.exceptions.get(0);
    	}

		LogManager.logInfo(LogConstants.CTX_CONNECTOR, this.command);

		DBCollection collection = this.mongoDB.getCollection(this.visitor.collectionTable.getName());
		if (collection != null) {
			// TODO: check to see how to pass the hint
			ArrayList<DBObject> ops = new ArrayList<DBObject>();
			if (this.visitor.projectBeforeMatch) {
				buildAggregate(ops, "$project", this.visitor.project); //$NON-NLS-1$
			}

			if (!this.visitor.unwindTables.isEmpty()) {
				for (String name:this.visitor.unwindTables) {
					buildAggregate(ops, "$unwind", "$"+name); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			buildAggregate(ops, "$match", this.visitor.match); //$NON-NLS-1$

			buildAggregate(ops, "$group", this.visitor.group); //$NON-NLS-1$
			buildAggregate(ops, "$match", this.visitor.having); //$NON-NLS-1$

			if (!this.visitor.projectBeforeMatch) {
				buildAggregate(ops, "$project", this.visitor.project); //$NON-NLS-1$
			}
			buildAggregate(ops, "$sort", this.visitor.sort); //$NON-NLS-1$
			buildAggregate(ops, "$skip", this.visitor.skip); //$NON-NLS-1$
			buildAggregate(ops, "$limit", this.visitor.limit); //$NON-NLS-1$

			AggregationOutput output = collection.aggregate(ops.remove(0), ops.toArray(new DBObject[ops.size()]));
			this.results = output.results().iterator();
		}
	}

	private void buildAggregate(List<DBObject> query, String type, Object object) {
		if (object != null) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, type+":"+object.toString()); //$NON-NLS-1$
			query.add(new BasicDBObject(type, object));
		}
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		if (this.results != null && this.results.hasNext()) {
			DBObject result = this.results.next();
			if (result != null) {
				ArrayList row = new ArrayList();
				for (int i = 0; i < this.visitor.selectColumns.size();i++) {
					row.add(this.executionFactory.retrieveValue(result.get(this.visitor.selectColumns.get(i)), this.expectedTypes[i], this.mongoDB, this.visitor.selectColumns.get(i), this.visitor.selectColumnReferences.get(i)));
				}
				return row;
			}
		}
		return null;
	}

	@Override
	public void close() {
		this.results = null;
	}

	@Override
	public void cancel() throws TranslatorException {
		close();
	}
}
