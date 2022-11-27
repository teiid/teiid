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
package org.teiid.translator.infinispan.hotrod;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.infinispan.api.DocumentFilter;
import org.teiid.infinispan.api.DocumentFilter.Action;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.infinispan.api.InfinispanPlugin;
import org.teiid.infinispan.api.MarshallerBuilder;
import org.teiid.infinispan.api.ProtobufMetadataProcessor;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.Insert;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.infinispan.hotrod.InfinispanUpdateVisitor.OperationType;


public class InfinispanUpdateExecution implements UpdateExecution {
    private int updateCount = 0;
    private Command command;
    private InfinispanConnection connection;
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;

    public InfinispanUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata,
            InfinispanConnection connection) {
        this.command = command;
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
    }

    @Override
    public void execute() throws TranslatorException {
        final InfinispanUpdateVisitor visitor = new InfinispanUpdateVisitor(this.metadata);
        visitor.append(this.command);

        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        Table table = visitor.getParentTable();

        Column pkColumn = visitor.getPrimaryKey();
        if (pkColumn == null) {
            throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25013, table.getName()));
        }

        final String PK = MarshallerBuilder.getDocumentAttributeName(pkColumn, false, this.metadata);

        DocumentFilter docFilter = createDocumentFilter(visitor);

        this.connection.registerMarshaller(table, this.metadata);

        // if the message in defined in different cache than the default, switch it out now.
        final RemoteCache<Object, Object> cache = InfinispanQueryExecution.getCache(table, connection);

        if (visitor.getOperationType() == OperationType.DELETE) {
            paginateDeleteResults(cache, visitor.getDeleteQuery(), new Task() {
                @Override
                public void run(Object row) throws TranslatorException {
                    if (visitor.isNestedOperation()) {
                        String childName = ProtobufMetadataProcessor.getMessageName(visitor.getQueryTable());
                        InfinispanDocument document = (InfinispanDocument)row;
                        InfinispanResponse.applyFilter(document, docFilter);
                        cache.replace(document.getProperties().get(PK), document);
                        // false below means count that not matched, i.e. deleted count
                        updateCount = updateCount + document.getUpdateCount(childName, false);
                    } else {
                        Object key = ((Object[])row)[0];
                        cache.remove(key);
                        updateCount++;
                    }
                }
            }, this.executionContext.getBatchSize());
        } else if (visitor.getOperationType() == OperationType.UPDATE) {
            paginateUpdateResults(cache, visitor.getUpdateQuery(), new Task() {
                @Override
                public void run(Object row) throws TranslatorException {
                    InfinispanDocument previous = (InfinispanDocument)row;
                    InfinispanResponse.applyFilter(previous, docFilter);
                    Object key = previous.getProperties().get(PK);
                    int count = previous.merge(visitor.getInsertPayload());
                    cache.replace(key, previous);
                    updateCount = updateCount + count;
                }
            }, this.executionContext.getBatchSize());
        } else if (visitor.getOperationType() == OperationType.INSERT) {
            performInsert(visitor, table, cache, false);
        } else if (visitor.getOperationType() == OperationType.UPSERT) {
            performInsert(visitor, table, cache, true);
        }
    }

    private DocumentFilter createDocumentFilter(
            final InfinispanUpdateVisitor visitor) throws TranslatorException {
        if (!visitor.isNestedOperation() || visitor.getWhereClause() == null) {
            return null;
        }
        Action action = Action.ALWAYSADD;
        if (command instanceof Delete) {
            action = Action.REMOVE;
        }
        SQLStringVisitor ssv = new SQLStringVisitor() {

            @Override
            public void visit(ColumnReference obj) {
                String groupName = null;
                NamedTable group = obj.getTable();
                if(group.getCorrelationName() != null) {
                    groupName = group.getCorrelationName();
                } else {
                    Table groupID = group.getMetadataObject();
                    if (groupID.getFullName().equals(visitor.getParentTable().getFullName())) {
                        groupName = visitor.getParentNamedTable().getCorrelationName();
                    } else {
                        groupName = visitor.getQueryNamedTable().getCorrelationName();
                    }
                }
                buffer.append(groupName).append(Tokens.DOT).append(getName(obj.getMetadataObject()));
            }

            @Override
            public String getName(AbstractMetadataRecord object) {
                return object.getName();
            }
        };
        ssv.append(visitor.getWhereClause());

        return new ComplexDocumentFilter(visitor.getParentNamedTable(), visitor.getQueryNamedTable(),
                this.metadata, ssv.toString(), action);
    }

    @SuppressWarnings("unchecked")
    private void performInsert(final InfinispanUpdateVisitor visitor, Table table,
            final RemoteCache<Object, Object> cache, boolean upsert)
            throws TranslatorException {
        Insert insert = (Insert)this.command;
        if (visitor.isNestedOperation()) {
            InfinispanDocument previous = null;
            if (visitor.getIdentity() != null) {
                previous = (InfinispanDocument)cache.get(visitor.getIdentity());
            }
            if (insert.getParameterValues() != null) {
                throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25017,
                        table.getName(), visitor.getParentTable().getName()));
            }
            if (previous == null) {
                throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25009,
                        table.getName(), visitor.getIdentity()));
            }
            String childName = ProtobufMetadataProcessor.getMessageName(visitor.getQueryTable());
            previous.addChildDocument(childName, visitor.getInsertPayload().getChildDocuments(childName).get(0));
            if (upsert) {
                previous = (InfinispanDocument) cache.replace(visitor.getIdentity(), previous);
            } else {
                previous = (InfinispanDocument) cache.put(visitor.getIdentity(), previous);
            }
            this.updateCount++;
        } else {
            if (insert.getParameterValues() == null) {
                insertRow(cache, visitor.getIdentity(), visitor.getInsertPayload(), upsert);
                this.updateCount++;
            } else {

                boolean putAll = upsert;
                if (this.executionContext.getSourceHint() != null) {
                    putAll = this.executionContext.getSourceHint().indexOf("use-putall") != -1;
                }

                // bulk insert
                int batchSize = this.executionContext.getBatchSize();
                Iterator<? extends List<Expression>> args = (Iterator<? extends List<Expression>>) insert
                        .getParameterValues();
                while(true) {
                    Map<Object, InfinispanDocument> rows = visitor.getBulkInsertPayload(insert, batchSize, args);
                    if (rows.isEmpty()) {
                        break;
                    }
                    BulkInsert bi = null;
                    if (putAll) {
                        bi = new UsePutAll(cache);
                    } else {
                        bi = new OneAtATime(cache);
                    }
                    bi.run(rows, upsert);
                    this.updateCount+=rows.size();
                }
            }
        }
    }

    private void insertRow(RemoteCache<Object, Object> cache, Object rowKey, InfinispanDocument row, boolean upsert)
            throws TranslatorException {
        // this is always single row; putIfAbsent is not working correctly.
        InfinispanDocument previous = (InfinispanDocument) cache.get(rowKey);
        if (previous != null && !upsert) {
            throw new TranslatorException(
                    InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25005, previous.getName(), rowKey));
        }
        if (upsert && previous != null) {
            previous.merge(row);
            previous = (InfinispanDocument) cache.replace(rowKey, previous);
        } else {
            previous = row;
            previous = (InfinispanDocument) cache.put(rowKey, previous);
        }
    }

    interface BulkInsert {
        long run(Map<Object, InfinispanDocument> rows, boolean upsert)
                throws TranslatorException;
    }

    interface Task {
        void run(Object rows) throws TranslatorException;
    }

    /*
    private class ExecutionBasedBulkInsert implements BulkInsert {
        private TeiidTableMarshaller marshaller;
        private InfinispanConnection connection;

        public ExecutionBasedBulkInsert(InfinispanConnection connection, TeiidTableMarshaller marshaller)
                throws TranslatorException {
            this.marshaller = marshaller;
            this.connection = connection;
        }

        @Override
        public long run(Map<Object, InfinispanDocument> rows, boolean upsert) throws TranslatorException {
            try {
                HashMap<String, Object> parameters = new HashMap<>();
                parameters.put("upsert", String.valueOf(upsert));
                parameters.put("row-count", rows.size());
                int count = 0;
                for (Map.Entry<Object, InfinispanDocument> document:rows.entrySet()) {
                    parameters.put("row-key-"+count, document.getKey());

                    ByteArrayOutputStream out;
                    try {
                        out = new ByteArrayOutputStream(10*1024);
                        RawProtoStreamWriter writer = RawProtoStreamWriterImpl.newInstance(out);
                        this.marshaller.writeTo(null, writer, document.getValue());
                        writer.flush();
                        out.close();
                    } catch (IOException e) {
                        throw new TranslatorException(e);
                    }
                    parameters.put("row-"+count, out.toByteArray());
                    count++;
                }
                return this.connection.execute("teiid-bulk-insert", parameters);
            } catch (RuntimeException e) {
                throw new TranslatorException(e);
            }
        }
    }
    */

    private class OneAtATime implements BulkInsert {
        private RemoteCache<Object, Object> cache;

        public OneAtATime(RemoteCache<Object, Object> cache) {
            this.cache = cache;
        }

        @Override
        public long run(Map<Object, InfinispanDocument> rows, boolean upsert) throws TranslatorException {
            long updateCount = 0;
            for (Map.Entry<Object, InfinispanDocument> row : rows.entrySet()) {
                insertRow(this.cache, row.getKey(), row.getValue(), upsert);
                updateCount++;
            }
            return updateCount;
        }
    }

    private class UsePutAll implements BulkInsert {
        private RemoteCache<Object, Object> cache;

        public UsePutAll(RemoteCache<Object, Object> cache) {
            this.cache = cache;
        }

        @Override
        public long run(Map<Object, InfinispanDocument> rows, boolean upsert) throws TranslatorException {
            this.cache.putAll(rows);
            return rows.size();
        }
    }

    /*
     * pagination for delete does not need to use the offset, because when a delete is done, the subsequent query does not include
     * the previously removed objects.
     */
       static void paginateDeleteResults(RemoteCache<Object, Object> cache, String queryStr, Task task, int batchSize) throws TranslatorException {

            if (cache.isEmpty()) return;

            QueryFactory qf = Search.getQueryFactory(cache);
            Query query = qf.create(queryStr);

            int offset = 0;
            while (true) {
                query.startOffset(offset);
                query.maxResults(batchSize);

                List<Object> values = query.list();

                if (values == null || values.isEmpty()) {
                    break;
                }
                for (Object doc : values) {
                    task.run(doc);
                }
            }
        }

        /*
         * pagination for update options does use the offset, because the query returns the same set of objects, so the
         * offset has to be used to position the next group of objects to be updated
         */

    static void paginateUpdateResults(RemoteCache<Object, Object> cache, String queryStr, Task task, int batchSize)
            throws TranslatorException {

        QueryFactory qf = Search.getQueryFactory(cache);
        Query query = qf.create(queryStr);

        int offset = 0;
        query.startOffset(0);
        query.maxResults(batchSize);
        List<Object> values = query.list();
        while (true) {
            for(Object doc : values) {
                task.run(doc);
            }
            if (query.getResultSize() < batchSize) {
                break;
            }
            offset = offset + batchSize;
            query.startOffset(offset);
            values = query.list();
        }

    }


    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return new int[] {this.updateCount};
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }
}
