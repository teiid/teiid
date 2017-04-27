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
package org.teiid.translator.infinispan.hotrod;

import java.util.List;
import java.util.Map.Entry;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Delete;
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
import org.teiid.translator.document.Document;
import org.teiid.translator.infinispan.hotrod.DocumentFilter.Action;
import org.teiid.translator.infinispan.hotrod.InfinispanUpdateVisitor.OperationType;

public class InfinispanUpdateExecution implements UpdateExecution {
    private int updateCount = 0;
    private Command command;
    private InfinispanConnection connection;
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;

    public InfinispanUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata,
            InfinispanConnection connection) throws TranslatorException {
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

        TeiidTableMarsheller marshaller = null;
        try {
            Table table = visitor.getParentTable();
            Column pkColumn = visitor.getPrimaryKey();
            if (pkColumn == null) {
                throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25013, table.getName()));
            }

            final String PK = MarshallerBuilder.getDocumentAttributeName(pkColumn, false, this.metadata);

            DocumentFilter docFilter = null;
            if (visitor.isNestedOperation() && visitor.getWhereClause() != null) {
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

                docFilter = new ComplexDocumentFilter(visitor.getParentNamedTable(), visitor.getQueryNamedTable(),
                        this.metadata, ssv.toString(), action);
            }

            marshaller = MarshallerBuilder.getMarshaller(table, this.metadata, docFilter);
            this.connection.registerMarshaller(marshaller);

            // if the message in defined in different cache than the default, switch it out now.
            final RemoteCache<Object,Object> cache = InfinispanQueryExecution.getCache(table, connection);


            if (visitor.getOperationType() == OperationType.DELETE) {
                paginateResults(cache, visitor.getDeleteQuery(), new Task() {
                    @Override
                    public void run(Object row) throws TranslatorException {
                        if (visitor.isNestedOperation()) {
                            String childName = ProtobufMetadataProcessor.getMessageName(visitor.getQueryTable());
                            InfinispanDocument document = (InfinispanDocument)row;
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
                paginateResults(cache, visitor.getUpdateQuery(), new Task() {
                    @Override
                    public void run(Object row) throws TranslatorException {
                        InfinispanDocument previous = (InfinispanDocument)row;
                        int count = mergeUpdatePayload(previous, visitor.getInsertPayload());
                        cache.replace(previous.getProperties().get(PK), previous);
                        updateCount = updateCount + count;
                    }
                }, this.executionContext.getBatchSize());
            } else if (visitor.getOperationType() == OperationType.INSERT) {
                InfinispanDocument previous = (InfinispanDocument)cache.get(visitor.getIdentity());
                if (visitor.isNestedOperation()) {
                    if (previous == null) {
                        throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25009,
                                table.getName(), visitor.getIdentity()));
                    }
                    String childName = ProtobufMetadataProcessor.getMessageName(visitor.getQueryTable());
                    previous.addChildDocument(childName, visitor.getInsertPayload().getChildDocuments(childName).get(0));
                } else {
                    // this is always single row; putIfAbsent is not working correctly.
                    if (previous != null) {
                        throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25005,
                                table.getName(), visitor.getIdentity()));
                    }
                    previous = visitor.getInsertPayload();
                }
                previous = (InfinispanDocument) cache.put(visitor.getIdentity(), previous);
                this.updateCount++;
            } else if (visitor.getOperationType() == OperationType.UPSERT) {
                boolean replace = false;
                // this is always single row; putIfAbsent is not working correctly.
                InfinispanDocument previous = (InfinispanDocument)cache.get(visitor.getIdentity());
                if (visitor.isNestedOperation()) {
                    if (previous == null) {
                        throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25009,
                                table.getName(), visitor.getIdentity()));
                    }
                    String childName = ProtobufMetadataProcessor.getMessageName(visitor.getQueryTable());
                    previous.addChildDocument(childName, visitor.getInsertPayload().getChildDocuments(childName).get(0));
                    replace = true;
                } else {
                    if (previous != null) {
                        mergeUpdatePayload(previous, visitor.getInsertPayload());
                        replace = true;
                    } else {
                        previous = visitor.getInsertPayload();
                    }
                }
                if (replace) {
                    previous = (InfinispanDocument) cache.replace(visitor.getIdentity(), previous);
                } else {
                    previous = (InfinispanDocument) cache.put(visitor.getIdentity(), previous);
                }
                this.updateCount++;
            }
        } finally {
            if (marshaller != null) {
                this.connection.unRegisterMarshaller(marshaller);
            }
        }
    }

    interface Task {
        void run(Object rows) throws TranslatorException;
    }

    static void paginateResults(RemoteCache<Object, Object> cache, String queryStr, Task task, int batchSize)
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

    private int mergeUpdatePayload(InfinispanDocument previous,
            InfinispanDocument updates) {
        int updated = 1;
        for (Entry<String, Object> entry:updates.getProperties().entrySet()) {
            previous.addProperty(entry.getKey(), entry.getValue());
        }

        // update children if any
        for (Entry<String, List<Document>> entry:updates.getChildren().entrySet()) {
            String childName = entry.getKey();

            List<? extends Document> childUpdates = updates.getChildDocuments(childName);
            InfinispanDocument childUpdate = (InfinispanDocument)childUpdates.get(0);
            if (childUpdate.getProperties().isEmpty()) {
                continue;
            }

            List<? extends Document> previousChildren = previous.getChildDocuments(childName);
            if (previousChildren == null || previousChildren.isEmpty()) {
                previous.addChildDocument(childName, childUpdate);
            } else {
                for (Document doc : previousChildren) {
                    InfinispanDocument previousChild = (InfinispanDocument)doc;
                    if (previousChild.isMatched()) {
                        for (Entry<String, Object> childEntry:childUpdate.getProperties().entrySet()) {
                            String key = childEntry.getKey().substring(childEntry.getKey().lastIndexOf('/')+1);
                            previousChild.addProperty(key, childEntry.getValue());
                            updated++;
                        }
                    }
                }
            }
        }
        return updated;
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
