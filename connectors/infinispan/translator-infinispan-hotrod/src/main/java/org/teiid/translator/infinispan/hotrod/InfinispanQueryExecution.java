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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.api.BasicCache;
import org.teiid.infinispan.api.DocumentFilter;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.TeiidTableMarsheller;
import org.teiid.infinispan.api.DocumentFilter.Action;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class InfinispanQueryExecution implements ResultSetExecution {

    private Select command;
    private InfinispanConnection connection;
    private RuntimeMetadata metadata;
    private ExecutionContext executionContext;
    private InfinispanResponse results;
    private TeiidTableMarsheller marshaller;
    private boolean useAliasCache;

	public InfinispanQueryExecution(InfinispanExecutionFactory translator, QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata, InfinispanConnection connection,
			boolean useAliasCache) throws TranslatorException {
        this.command = (Select)command;
        this.connection = connection;
        this.metadata = metadata;
        this.executionContext = executionContext;
        this.useAliasCache = useAliasCache;
    }

    @Override
    public void execute() throws TranslatorException {
        try {
            if (useAliasCache) {
            	useModifiedGroups(this.connection, this.executionContext, this.metadata, this.command);
            }
            
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
			results = new InfinispanResponse(cache, queryStr, this.executionContext.getBatchSize(),
					visitor.getRowLimit(), visitor.getRowOffset(), visitor.getProjectedDocumentAttributes(),
					visitor.getDocumentNode());
        } finally {
            this.connection.unRegisterMarshaller(this.marshaller);
        }
    }

	static void useModifiedGroups(InfinispanConnection connection, ExecutionContext context, RuntimeMetadata metadata,
			Command command) throws TranslatorException {
		BasicCache<String, String> aliasCache = InfinispanDirectQueryExecution.getAliasCache(connection);
		CollectorVisitor.collectGroups(command).forEach(namedTable -> {
			try {
				Table table = InfinispanDirectQueryExecution.getAliasTable(context, metadata, aliasCache,
						namedTable.getMetadataObject());
				Collection<ColumnReference> columns = CollectorVisitor.collectElements(command);
				columns.forEach(reference -> {
					if (reference.getTable().getMetadataObject().equals(namedTable.getMetadataObject())) {
						Column column = table.getColumnByName(reference.getMetadataObject().getName());
						reference.getTable().setMetadataObject(table);
						reference.setMetadataObject(column);
					}
				});
				namedTable.setMetadataObject(table);
			} catch (TranslatorException e) {
				LogManager.logError(LogConstants.CTX_CONNECTOR, e, e.getMessage());
			}
		});
	}

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        try {
            this.connection.registerMarshaller(this.marshaller);
            return results.getNextRow();
        } catch(IOException e) {
        	throw new TranslatorException(e);
        }
        finally {
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
            cache = (RemoteCache<Object, Object>)connection.getCache(cacheName, true);
            if (cache == null) {
            	throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25020, cacheName));
            }
        }
        return cache;
    }
}
