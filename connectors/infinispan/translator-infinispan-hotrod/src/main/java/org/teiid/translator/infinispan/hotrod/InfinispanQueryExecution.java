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

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.DocumentFilter.Action;

public class InfinispanQueryExecution implements ResultSetExecution {

    private QueryExpression command;
    private InfinispanConnection connection;
    private RuntimeMetadata metadata;
    private ExecutionContext executionContext;
    private InfinispanResponse results;
    private TeiidTableMarsheller marshaller;

    public InfinispanQueryExecution(InfinispanExecutionFactory translator,
            QueryExpression command, ExecutionContext executionContext,
            RuntimeMetadata metadata, InfinispanConnection connection) throws TranslatorException {
        this.command = command;
        this.connection = connection;
        this.metadata = metadata;
        this.executionContext = executionContext;
    }

    @Override
    public void execute() throws TranslatorException {
        try {
            final IckleConversionVisitor visitor = new IckleConversionVisitor(metadata, false);
            visitor.append(this.command);
            Table table = visitor.getParentTable();
            String queryStr = visitor.getQuery();
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "SourceQuery:", queryStr);

            DocumentFilter docFilter = null;
            if (queryStr.startsWith("FROM ") && ((Select)command).getWhere() != null) {
                SQLStringVisitor ssv = new SQLStringVisitor() {
                    @Override
                    public String getName(AbstractMetadataRecord object) {
                        return object.getName();
                    }
                };
                ssv.append(((Select)command).getWhere());
                docFilter = new ComplexDocumentFilter(visitor.getParentNamedTable(), visitor.getQueryNamedTable(),
                        this.metadata, ssv.toString(), Action.ADD);
            }

            this.marshaller = MarshallerBuilder.getMarshaller(table, this.metadata, docFilter);
            this.connection.registerMarshaller(this.marshaller);

            // if the message in defined in different cache than the default, switch it out now.
            RemoteCache<Object, Object> cache =  getCache(table, connection);
            results = new InfinispanResponse(cache, queryStr, this.executionContext.getBatchSize(), visitor.getRowLimit(),
                    visitor.getRowOffset(), visitor.getProjectedDocumentAttributes(), visitor.getDocumentNode());
        } finally {
            this.connection.unRegisterMarshaller(this.marshaller);
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        try {
            this.connection.registerMarshaller(this.marshaller);
            return results.getNextRow();
        } finally {
            this.connection.unRegisterMarshaller(this.marshaller);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    static RemoteCache<Object, Object> getCache(Table table, InfinispanConnection connection) throws TranslatorException {
        RemoteCache<Object, Object> cache = (RemoteCache<Object, Object>)connection.getCache();
        String cacheName = table.getProperty(ProtobufMetadataProcessor.CACHE, false);
        if (cacheName != null && !cacheName.equals(connection.getCache().getName())) {
            cache = ((RemoteCacheManager)connection.getCacheFactory()).getCache(cacheName);
        }
        return cache;
    }
}
