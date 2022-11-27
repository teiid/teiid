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

import static org.teiid.language.visitor.SQLStringVisitor.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.GeneratedKeys;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.language.Update;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
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
import org.teiid.translator.mongodb.MergeDetails.Association;

import com.mongodb.*;

public class MongoDBUpdateExecution extends MongoDBBaseExecution implements UpdateExecution {
    private Command command;
    private MongoDBUpdateVisitor visitor;
    private MongoDBExecutionFactory executionFactory;
    private int[] results = new int[] {0};

    public MongoDBUpdateExecution(MongoDBExecutionFactory executionFactory,
            Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            MongoDBConnection connection) throws TranslatorException {
        super(executionContext, metadata, connection);
        this.command = command;
        this.executionFactory = executionFactory;

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
        try {
            executeInternal();
        } catch (MongoException e) {
            throw new TranslatorException(e);
        }
    }

    private void executeInternal() throws TranslatorException {

        DBCollection collection = getCollection(this.visitor.mongoDoc.getTargetTable());
        MongoDocument mongoDoc = this.visitor.mongoDoc;
        AggregationOptions options = this.executionFactory.getOptions(this.executionContext.getBatchSize());

        List<WriteResult> executionResults = new ArrayList<WriteResult>();

        if (this.command instanceof Insert) {
            // get pull key based documents to embed
            LinkedHashMap<String, DBObject> embeddedDocuments = fetchEmbeddedDocuments();

            // check if this document need to be embedded in any other document
            if (mongoDoc.isMerged()) {
                DBObject match = getInsertMatch(mongoDoc, this.visitor.columnValues);
                BasicDBObject insert = this.visitor.getInsert(embeddedDocuments);

                if (mongoDoc.getMergeKey().getAssociation() == Association.MANY) {
                    removeParentKey(mongoDoc, insert);
                    BasicDBObject insertDoc = new BasicDBObject(mongoDoc.getQualifiedName(true), insert);
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+match+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$push\": {"+insertDoc+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    executionResults.add(collection.update(match, new BasicDBObject("$push", insertDoc), false, true, WriteConcern.ACKNOWLEDGED)); //$NON-NLS-1$
                }
                else {
                    insert.remove("_id"); //$NON-NLS-1$
                    BasicDBObject insertDoc = new BasicDBObject(mongoDoc.getQualifiedName(true), insert);
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+match+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$set\": {"+insertDoc+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    executionResults.add(collection.update(match, new BasicDBObject("$set", insertDoc), false, true, WriteConcern.ACKNOWLEDGED)); //$NON-NLS-1$
                }
            }
            else {
                for (String docName:embeddedDocuments.keySet()) {
                    DBObject embeddedDoc = embeddedDocuments.get(docName);
                    embeddedDoc.removeField("_id"); //$NON-NLS-1$
                }
                // gets its own collection
                BasicDBObject in = this.visitor.getInsert(embeddedDocuments);
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "{\"insert\": {"+in+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                executionResults.add(collection.insert(in, WriteConcern.ACKNOWLEDGED));
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
                // multi items in array update not available, http://jira.mongodb.org/browse/SERVER-1243
                // this work-around for above issue
                List<String> parentKeyNames = parentKeyNames(mongoDoc);

                DBObject documentMatch = new BasicDBObject("$match", match); //$NON-NLS-1$
                DBObject projection = new BasicDBObject("$project", buildProjectForUpdate(mongoDoc)); //$NON-NLS-1$
                Cursor output = collection.aggregate(Arrays.asList(documentMatch, projection), options);
                while(output.hasNext()) {
                    BasicDBObject row = (BasicDBObject)output.next();
                    buildUpdate(mongoDoc, collection, row, parentKeyNames, 0, null, executionResults, new UpdateOperationImpl());
                }
            }
            else {
                for (String docName:embeddedDocuments.keySet()) {
                    DBObject embeddedDoc = embeddedDocuments.get(docName);
                    embeddedDoc.removeField("_id"); //$NON-NLS-1$
                }
                BasicDBObject u = this.visitor.getUpdate(embeddedDocuments);
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+match+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$set\": {"+u+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                executionResults.add(collection.update(match, new BasicDBObject("$set", u), false, true, WriteConcern.ACKNOWLEDGED)); //$NON-NLS-1$
            }

            // if the update is for the "embeddable" table, then since it is copied to other tables
            // those references need to be updated. I know this is not atomic operation, but not sure
            // how else to handle it.
            if (mongoDoc.isEmbeddable()) {
                updateReferenceTables(collection, mongoDoc, match, options);
            }
        }
        else {
            // Delete
            DBObject match = new BasicDBObject();
            if (this.visitor.match != null) {
                match = this.visitor.match;
            }

            if (mongoDoc.isEmbeddable()) {
                DBObject m = new BasicDBObject("$match", match); //$NON-NLS-1$
                Cursor output = collection.aggregate(Arrays.asList(m), options);
                while(output.hasNext()) {
                    DBObject row = output.next();
                    if (row != null) {
                        for (MergeDetails ref:mongoDoc.getEmbeddedIntoReferences()) {
                            DBCollection parent = getCollection(ref.getParentTable());
                            DBObject parentMatch = buildParentMatch(row, ref);
                            DBObject refMatch = new BasicDBObject("$match", parentMatch); //$NON-NLS-1$
                            Cursor referenceOutput = parent.aggregate(Arrays.asList(refMatch), options);
                            if (referenceOutput.hasNext()) {
                                throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18010, this.visitor.mongoDoc.getTargetTable().getName(), ref.getParentTable()));
                            }
                        }
                    }
                }
            }

            if (mongoDoc.isMerged()) {
                List<String> parentKeyNames = parentKeyNames(mongoDoc);

                DBObject documentMatch = new BasicDBObject("$match", match); //$NON-NLS-1$
                DBObject projection = new BasicDBObject("$project", buildProjectForUpdate(mongoDoc)); //$NON-NLS-1$
                Cursor output = collection.aggregate(Arrays.asList(documentMatch, projection), options);
                while(output.hasNext()) {
                    BasicDBObject row = (BasicDBObject)output.next();
                    buildUpdate(mongoDoc, collection, row, parentKeyNames, 0, null, executionResults, new DeleteOperationImpl(match));
                }
            }
            else {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "remove - {\"$match\": {"+match+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                executionResults.add(collection.remove(match, WriteConcern.ACKNOWLEDGED));
            }
        }

        if (!executionResults.isEmpty()) {
            if (this.command instanceof Insert) {
                addAutoGeneretedKeys(executionResults.get(0));
            }

            int updated = 0;
            for (WriteResult result:executionResults) {
                updated +=result.getN();
            }

            this.results = new int[1];
            this.results[0] = updated;
        }
    }

    DBObject getInsertMatch(MongoDocument mongoDocument, Map<String, Object> values) throws TranslatorException {
        List<DBObject> matches = new ArrayList<DBObject>();
        HashMap<String, Object> matchValues = new HashMap<String, Object>();
        MongoDocument mergeDocument = mongoDocument.getMergeDocument();
        MongoDocument targetDocument = mongoDocument.getTargetDocument();

        if (mongoDocument.getMergeKey().getAssociation() == Association.ONE) {
            while(mergeDocument.isMerged()) {
                matches.add(QueryBuilder.start(mergeDocument.getDocumentName()).exists(true).get());
                if (mergeDocument.getMergeKey().getAssociation() == Association.ONE) {
                    mergeDocument = mergeDocument.getMergeDocument();
                }
                else {
                    break;
                }
            }
            if (mergeDocument.equals(targetDocument)) {
                matchValues.put("_id", values.get("_id")); //$NON-NLS-1$ //$NON-NLS-2$
            }
            else {
                matchValues.put(mergeDocument+"._id", values.get("_id")); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        else {
            // this nested so add exists
            if (!mergeDocument.equals(targetDocument)) {
                matches.add(QueryBuilder.start(mergeDocument.getDocumentName()).exists(true).get());
            }

            // this value can go only one level up
            String columnName = mergeDocument.getColumnName(mongoDocument.getMergeKey().getReferenceColumns().get(0));
            Object value = values.get(mongoDocument.getMergeKey().getColumns().get(0));
            if (value instanceof MergeDetails) {
                value = ((MergeDetails)value).getValue();
            }

            if (mergeDocument.equals(targetDocument)) {
                matchValues.put(columnName, value);
            }
            else {
                while(mergeDocument.isMerged()) {
                    if (mergeDocument.getMergeKey().getAssociation() == Association.ONE) {
                        mergeDocument = mergeDocument.getMergeDocument();
                    }
                    else {
                        break;
                    }
                }
                matchValues.put(columnName, value);
            }
        }

        // build match
        BasicDBObject match = new BasicDBObject();
        for (String key:matchValues.keySet()) {
            match.append(key, matchValues.get(key));
        }
        if (!matches.isEmpty()) {
            matches.add(match);
            QueryBuilder qb = QueryBuilder.start().and(matches.toArray(new BasicDBObject[matches.size()]));
            match = (BasicDBObject)qb.get();
        }
        return match;
    }

    static class RowInfo {
        String tableName;
        String mergedTableName;
        Object PK;
        int rowNumber;
        RowInfo parent;
        boolean istop;

        static RowInfo build(String name, String mergeName, Object pk, int rowNumber, RowInfo parent) {
            RowInfo info = new RowInfo();
            info.tableName = name;
            info.mergedTableName = mergeName;
            info.PK = pk;
            info.rowNumber = rowNumber;
            if (parent != null) {
                info.parent = parent;
                info.istop = false;
            }
            else {
                info.istop = true;
            }
            return info;
        }

        public String getId(MongoDocument parent) {
            StringBuilder sb = new StringBuilder();
            sb.append(parent.getTable().getName());
            if (this.parent != null) {
                getId(sb);
            }
            return sb.toString();
        }

        private void getId(StringBuilder sb) {
            if (this.parent != null) {
                if (this.rowNumber != -1) {
                    sb.insert(0, "."); //$NON-NLS-1$
                    sb.insert(0,this.rowNumber);
                }
                this.parent.getId(sb);
            }
            if (!this.istop) {
                sb.insert(0, "."); //$NON-NLS-1$
                sb.insert(0, this.tableName);
            }
        }
    }

    private interface UpdateOperation {
        void execute(MongoDocument doc, DBCollection collection, DBObject row, DBObject dataRow,
            RowInfo rowInfo, List<WriteResult> executionResults) throws TranslatorException;
    }

    private void buildUpdate(MongoDocument doc, DBCollection collection, BasicDBObject row, List<String> parentKeys, int level,
            RowInfo rowInfo, List<WriteResult> executionResults, UpdateOperation operation) throws TranslatorException {

        String parentKeyName = parentKeys.get(level);
        boolean top = parentKeyName.equals(doc.getTargetDocument().getQualifiedName(false));
        Object parentBlock = row.get(top?"_id":parentKeyName); //$NON-NLS-1$

        // the parent-child must have been one-2-one relationship
        if (parentBlock == null) {
            parentBlock = rowInfo.PK;
        }
        String mergeTableName = doc.getTable().getName();
        if (parentKeys.size() != (level+1)) {
            mergeTableName = parentKeys.get(level+1);
        }

        if (parentBlock instanceof BasicDBList) {
            // so parent is an array document
            BasicDBList parentRows = (BasicDBList)parentBlock;
            //parentRows = (BasicDBList)((BasicDBObject)parentRows.get(0)).get("_id"); //$NON-NLS-1$
            for (int i = 0; i < parentRows.size(); i++) {
                RowInfo info = RowInfo.build(parentKeyName,mergeTableName, parentRows.get(i), i, rowInfo);
                if (parentKeys.size() == (level+1)) {
                    String aliasDocumentName = doc.getQualifiedName(false).replace('.', '_');
                    BasicDBList dataRows = (BasicDBList)row.get(aliasDocumentName);
                    if (dataRows != null && dataRows.size() > i) {
                        operation.execute(doc, collection, row, (DBObject)dataRows.get(i), info, executionResults);
                    }
                }
                else {
                    buildUpdate(doc, collection, row, parentKeys, level+1, info, executionResults, operation);
                }
            }
        }
        else {
            // here the _id is same as parent
            RowInfo info = RowInfo.build(parentKeyName, mergeTableName, parentBlock, -1, rowInfo);
            if (parentKeys.size() == (level+1)) {
                //Leaf, no more down
                String aliasDocumentName = doc.getQualifiedName(false).replace('.', '_');
                DBObject dataRows = (DBObject)row.get(aliasDocumentName);
                if (dataRows != null) {
                    operation.execute(doc, collection, row, dataRows, info, executionResults);
                }
            }
            else {
                buildUpdate(doc, collection, row, parentKeys, level+1, info, executionResults, operation);
            }
        }
    }

    class UpdateOperationImpl implements UpdateOperation {
        public void execute(MongoDocument doc, DBCollection collection, DBObject row, DBObject dataRow,
                RowInfo rowInfo, List<WriteResult> executionResults) throws TranslatorException {
            if (dataRow instanceof BasicDBList) {
                BasicDBList updatedDoc = new BasicDBList();
                boolean update = visitor.updateMerge((BasicDBList)dataRow, rowInfo, updatedDoc);
                if (update) {
                    BasicDBObject m = new BasicDBObject("_id", row.get("_id"));//$NON-NLS-1$ //$NON-NLS-2$
                    BasicDBObject u = new BasicDBObject(rowInfo.getId(doc), updatedDoc);
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+m+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$set\": {"+u+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    WriteResult result = collection.update(m, new BasicDBObject("$set", u), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
                    executionResults.add(result);
                }
            }
            else {
                BasicDBObject m = new BasicDBObject("_id", row.get("_id"));//$NON-NLS-1$ //$NON-NLS-2$
                boolean update = visitor.updateMerge((BasicDBObject)dataRow, rowInfo);
                if(update) {
                    BasicDBObject u = new BasicDBObject(rowInfo.getId(doc), dataRow);
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+m+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$set\": {"+u+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    WriteResult result = collection.update(m, new BasicDBObject("$set", u), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
                    executionResults.add(result);
                }
            }
        }
    }

    class DeleteOperationImpl implements UpdateOperation {
        private DBObject queryMatch;
        public DeleteOperationImpl(DBObject match) {
            this.queryMatch = match;
        }

        public void execute(MongoDocument doc, DBCollection collection, DBObject row, DBObject dataRow,
                RowInfo rowInfo, List<WriteResult> executionResults) throws TranslatorException {

            if (dataRow instanceof BasicDBList) {
                BasicDBObject pull = (BasicDBObject)visitor.getPullQuery().get(rowInfo.mergedTableName);
                if(this.queryMatch.keySet().isEmpty()) {
                    queryMatch = QueryBuilder.start(rowInfo.getId(doc)).exists(true).get();
                    pull = new BasicDBObject(rowInfo.getId(doc), pull != null ? pull : new BasicDBObject());
                    LogManager.logInfo(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+this.queryMatch+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    LogManager.logInfo(LogConstants.CTX_CONNECTOR, "update - {\"$pull\": {"+pull+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                    WriteResult result = collection.update(this.queryMatch, new BasicDBObject("$pull", pull), false, true, //$NON-NLS-1$
                            WriteConcern.ACKNOWLEDGED);
                    executionResults.add(result);
                }
                else {
                    BasicDBList updatedDoc = new BasicDBList();
                    boolean update = visitor.updateDelete((BasicDBList)dataRow, rowInfo, updatedDoc);
                    if (update) {
                        BasicDBObject m = new BasicDBObject("_id", row.get("_id"));//$NON-NLS-1$ //$NON-NLS-2$
                        BasicDBObject u = new BasicDBObject(rowInfo.getId(doc), updatedDoc);
                        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+m+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "update - {\"$set\": {"+u+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                        WriteResult result = collection.update(m, new BasicDBObject("$set", u), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
                        executionResults.add(result);
                    }
                }
            }
            else {
                if(this.queryMatch.keySet().isEmpty()) {
                    queryMatch = QueryBuilder.start(rowInfo.getId(doc)).exists(true).get();
                }
                BasicDBObject u = new BasicDBObject(rowInfo.getId(doc), ""); //$NON-NLS-1$
                LogManager.logInfo(LogConstants.CTX_CONNECTOR, "update - {\"$match\": {"+this.queryMatch+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                LogManager.logInfo(LogConstants.CTX_CONNECTOR, "update - {\"$unset\": {"+u+"}}"); //$NON-NLS-1$ //$NON-NLS-2$
                WriteResult result = collection.update(this.queryMatch, new BasicDBObject("$unset", u), false, true, WriteConcern.ACKNOWLEDGED); //$NON-NLS-1$
                executionResults.add(result);
            }
        }
    }

    private BasicDBObject buildProjectForUpdate(MongoDocument doc) throws TranslatorException {
        BasicDBObject project = new BasicDBObject();
        // the preview document for update
        String aliasDocumentName = doc.getQualifiedName(false).replace('.', '_');
        project.append(aliasDocumentName, "$"+doc.getQualifiedName(false)); //$NON-NLS-1$

        while (doc.isMerged()) {
            doc = doc.getMergeDocument();
            if (doc.isMerged()) {
                project.append(doc.getQualifiedName(false), "$"+doc.getQualifiedName(false)+"._id"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        project.append("_id", "$_id"); //$NON-NLS-1$ //$NON-NLS-2$
        return project;
    }

    private List<String> parentKeyNames(MongoDocument doc) throws TranslatorException {
        ArrayList<String> list = new ArrayList<String>();
        while (doc.isMerged()) {
            doc = doc.getMergeDocument();
            list.add(0, doc.getQualifiedName(false));
        }
        return list;
    }

    private void removeParentKey(MongoDocument document, BasicDBObject row) throws TranslatorException {
        Table source = document.getTable();
        Table target = document.getMergeTable();

        for (ForeignKey fk:source.getForeignKeys()) {
            if (fk.getReferenceTableName().equals(target.getName())){
                for (int i = 0; i < fk.getColumns().size(); i++) {
                    if (row != null) {
                        row.remove(fk.getColumns().get(i).getName());
                    }
                }
            }
        }
    }

    private void updateReferenceTables(DBCollection collection, MongoDocument mongoDoc,
            DBObject match, AggregationOptions options) throws TranslatorException {
        DBObject m = new BasicDBObject("$match", match); //$NON-NLS-1$
        Cursor output = collection.aggregate(Arrays.asList(m), options);
        while(output.hasNext()) {
            DBObject row = output.next();
            if (row != null) {
                for (MergeDetails ref:mongoDoc.getEmbeddedIntoReferences()) {
                    DBCollection parent = getCollection(ref.getParentTable());
                    //DBObject parentmatch = new BasicDBObject(ref.getReferenceName()+".$id", row.get("_id")); //$NON-NLS-1$ //$NON-NLS-2$
                    DBObject parentmatch = buildParentMatch(row, ref);
                    row.removeField("_id"); //$NON-NLS-1$
                    parent.update(parentmatch, new BasicDBObject("$set",new BasicDBObject(ref.getName(), row)), //$NON-NLS-1$
                            false, true, WriteConcern.ACKNOWLEDGED);

                    // see if there are nested references
                    Table parentTable = this.metadata.getTable(mongoDoc.getTable().getParent().getName(), ref.getParentTable());
                    MongoDocument parentMongoDocument = new MongoDocument(parentTable, this.metadata);
                    if (parentMongoDocument.isEmbeddable()) {
                        updateReferenceTables(parent, parentMongoDocument, parentmatch,options);
                    }
                }
            }
        }
    }

    private DBObject buildParentMatch(DBObject row, MergeDetails ref) {
        DBObject parentmatch = new BasicDBObject();
        Object rowid = row.get("_id"); //$NON-NLS-1$
        if (rowid instanceof BasicDBObject) {
            // composite key..
            for (int i = 0; i < ref.getColumns().size(); i++) {
                parentmatch.put(ref.getColumns().get(i), ((BasicDBObject) rowid).get(ref.getReferenceColumns().get(i)));
            }
        }
        else {
            parentmatch.put(ref.getColumns().get(0), rowid);
        }
        return parentmatch;
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
            key.append(getRecordName(c), 1);
        }
        BasicDBObject options = new BasicDBObject();
        options.put( "name" , record.getName()); //$NON-NLS-1$
        if (unique) {
            options.put( "unique" , Boolean.TRUE); //$NON-NLS-1$
        }
        collection.createIndex(key, options);
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return this.results;
    }


    private void addAutoGeneretedKeys(WriteResult result) throws TranslatorException {
        Table table = this.visitor.mongoDoc.getTargetTable();
        if (table.getPrimaryKey() == null) {
            return;
        }
        int cols = table.getPrimaryKey().getColumns().size();

        if (cols != 1) {
            //restrict to only primary keys based upon id
            return;
        }
        Class<?>[] columnDataTypes = new Class<?>[cols];
        String[] columnNames = new String[cols];
        //this is typically expected to be an int/long, but we'll be general here.  we may eventual need the type logic off of the metadata importer
        for (int i = 0; i < cols; i++) {
            columnDataTypes[i] = table.getPrimaryKey().getColumns().get(i).getJavaType();
            columnNames[i] = table.getPrimaryKey().getColumns().get(i).getName();
        }
        if (!columnNames[0].equals(MongoDBMetadataProcessor.ID)) {
            return;
        }
        GeneratedKeys generatedKeys = this.executionContext.getCommandContext().returnGeneratedKeys(columnNames, columnDataTypes);
        List<Object> vals = new ArrayList<Object>(columnDataTypes.length);
        for (int i = 0; i < columnDataTypes.length; i++) {
            Object value = this.executionFactory.retrieveValue(result.getUpsertedId(), columnDataTypes[i], this.mongoDB, columnNames[i], columnNames[i]);
            vals.add(value);
        }
        generatedKeys.addKey(vals);
    }
}
