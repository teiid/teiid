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

package org.teiid.resource.adapter.infinispan.hotrod;


import java.util.Map;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.infinispan.api.TeiidMarshallerProvider;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanManagedConnectionFactory.InfinispanConnectionFactory;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.ExecutionFactory.TransactionSupport;
import org.teiid.translator.TranslatorException;

public class InfinispanConnectionImpl extends BasicConnection implements InfinispanConnection {
    private RemoteCacheManager cacheManager;

    private BasicCache<?, ?> defaultCache;
    private TeiidMarshallerProvider ctx;
    private InfinispanConnectionFactory icf;
    private RemoteCacheManager scriptManager;
    private String cacheTemplate;

    public InfinispanConnectionImpl(RemoteCacheManager manager, RemoteCacheManager scriptManager, String cacheName,
            TeiidMarshallerProvider ctx, InfinispanConnectionFactory icf, String cacheTemplate) throws ResourceException {
        this.cacheManager = manager;
        this.ctx = ctx;
        this.icf = icf;
        this.scriptManager = scriptManager;
        this.cacheTemplate = cacheTemplate;

        try {
            this.defaultCache = getCache(cacheName);
        } catch (Throwable t) {
            throw new ResourceException(t);
        }
    }

    @Override
    public void registerProtobufFile(ProtobufResource protobuf) throws TranslatorException {
        this.icf.registerProtobufFile(protobuf, getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME));
    }

    @Override
    public void close() throws ResourceException {
    }

    @Override
    public BasicCache getCache() throws TranslatorException {
        return defaultCache;
    }

    @Override
    public <K, V> BasicCache<K, V> getCache(String cacheName) throws TranslatorException{
        if (ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME.equals(cacheName)) {
            //special handling for protobuf - don't create, and it can't be transactional
            return cacheManager.getCache(cacheName, TransactionMode.NONE);
        }

        TransactionMode transactionMode = icf.getTransactionMode();
        //check to see if the cache exists (required for in-vm testing).
        //We don't want to do this when transactional -
        //the ispn client will throw an exception if the cache does not exist.  while
        //we could just catch it, they still log a warning/error
        if (transactionMode == null || transactionMode == TransactionMode.NONE) {
            BasicCache<K, V> result = cacheName == null?cacheManager.getCache():cacheManager.getCache(cacheName);
            if (result != null) {
                return result;
            }
        }

        if (cacheName == null) {
            throw new TranslatorException("A default cache name is required to get a transactional cache or implicitly create the default cache."); //$NON-NLS-1$
        }
        if (cacheTemplate == null && transactionMode != null
                && transactionMode != TransactionMode.NONE) {
            //there doesn't seem to be a default transactional template, so
            //here's one - TODO externalize
            return cacheManager.administration().getOrCreateCache(cacheName, new XMLStringConfiguration(
                    "<infinispan><cache-container>" +
                    "  <distributed-cache-configuration name=\""+cacheName+"\">" +
                    "    <locking isolation=\"REPEATABLE_READ\"/>" +
                    "    <transaction locking=\"PESSIMISTIC\" mode=\""+transactionMode+"\" />" +
                    "  </distributed-cache-configuration>" +
                    "</cache-container></infinispan>"));
        }


        return cacheManager.administration().getOrCreateCache(cacheName, cacheTemplate);
    }

    @Override
    public void registerMarshaller(Table table, RuntimeMetadata metadata)
            throws TranslatorException {
        ctx.registerMarshaller(table, metadata);
    }

    @Override
    public <T> T execute(String scriptName, Map<String, ?> params) {
        return scriptManager.getCache().execute(scriptName, params);
    }

    @Override
    public TransactionSupport getTransactionSupport() {
        TransactionMode mode = icf.getTransactionMode();
        if (mode == null) {
            return TransactionSupport.NONE;
        }
        switch (mode) {
        case FULL_XA:
        case NON_DURABLE_XA:
            return TransactionSupport.XA;
        case NON_XA:
            return TransactionSupport.LOCAL;
        case NONE:
            return TransactionSupport.NONE;
        default:
            throw new IllegalArgumentException();
        }
    }

}