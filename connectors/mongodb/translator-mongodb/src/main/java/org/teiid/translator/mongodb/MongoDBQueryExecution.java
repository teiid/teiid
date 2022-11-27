/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.mongodb;

import java.util.ArrayList;
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

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class MongoDBQueryExecution extends MongoDBBaseExecution implements ResultSetExecution {
    private Select command;
    private MongoDBExecutionFactory executionFactory;
    private Cursor results;
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

        DBCollection collection = this.mongoDB.getCollection(this.visitor.mongoDoc.getTargetTable().getName());
        if (collection != null) {
            // TODO: check to see how to pass the hint
            ArrayList<DBObject> ops = new ArrayList<DBObject>();

            for (ProcessingNode ref:this.visitor.mergePlanner.getNodes()) {
                buildAggregate(ops, ref.getInstruction());
            }

            if (this.visitor.project.isEmpty()) {
                throw new TranslatorException(MongoDBPlugin.Event.TEIID18025, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18025));
            }

            assert visitor.selectColumns.size() == visitor.selectColumnReferences.size();

            if (this.visitor.projectBeforeMatch) {
                buildAggregate(ops, "$project", this.visitor.project); //$NON-NLS-1$
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

            try {
                this.results = collection.aggregate(ops, this.executionFactory.getOptions(this.executionContext.getBatchSize()));
            } catch (MongoException e) {
                throw new TranslatorException(e);
            }
        }
    }

    private void buildAggregate(List<DBObject> query, String type, Object object) {
        if (object != null) {
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "{\""+type+"\": {"+object.toString()+"}}"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            query.add(new BasicDBObject(type, object));
        }
    }

    private void buildAggregate(List<DBObject> query, DBObject dbObject) {
        if (dbObject != null) {
            query.add(dbObject);
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (this.results != null && this.results.hasNext()) {
            DBObject result = this.results.next();
            if (result != null) {
                int cols = this.visitor.selectColumns.size();
                ArrayList<Object> row = new ArrayList<>(cols);
                for (int i = 0; i < cols;i++) {
                    row.add(this.executionFactory.retrieveValue(result.get(this.visitor.selectColumns.get(i)), this.expectedTypes[i], this.mongoDB, this.visitor.selectColumns.get(i), this.visitor.selectColumnReferences.get(i)));
                }
                return row;
            }
        }
        return null;
    }

    @Override
    public void close() {
        if (this.results != null) {
            this.results.close();
            this.results = null;
        }
    }

    @Override
    public void cancel() throws TranslatorException {
        close();
    }
}
