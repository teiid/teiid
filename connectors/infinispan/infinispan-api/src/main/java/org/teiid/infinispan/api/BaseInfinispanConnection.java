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

package org.teiid.infinispan.api;


import java.util.Map;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionFactory.TransactionSupport;
import org.teiid.translator.TranslatorException;

public class BaseInfinispanConnection implements InfinispanConnection {
    private RemoteCacheManager cacheManager;

    private BasicCache<?, ?> defaultCache;
    private TeiidMarshallerProvider ctx;
    private InfinispanConnectionFactory icf;
    private RemoteCacheManager scriptManager;
    private String cacheTemplate;

    public BaseInfinispanConnection(RemoteCacheManager manager, RemoteCacheManager scriptManager, String cacheName,
            TeiidMarshallerProvider ctx, InfinispanConnectionFactory icf, String cacheTemplate) throws TranslatorException {
        this.cacheManager = manager;
        this.ctx = ctx;
        this.icf = icf;
        this.scriptManager = scriptManager;
        this.cacheTemplate = cacheTemplate;
        this.defaultCache = getCache(cacheName);
    }

    public BaseInfinispanConnection(InfinispanConnectionFactory icf) throws TranslatorException {
        this(icf.getCacheManager(), icf.getScriptCacheManager(),
                icf.getConfig().getCacheName(),
                icf.getTeiidMarshallerProvider(), icf,
                icf.getConfig().getCacheTemplate());
    }

    public void registerProtobufFile(ProtobufResource protobuf) throws TranslatorException {
        this.icf.registerProtobufFile(protobuf, getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME));
    }

    public BasicCache getCache() throws TranslatorException {
        return defaultCache;
    }

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

    public void registerMarshaller(Table table, RuntimeMetadata metadata)
            throws TranslatorException {
        ctx.registerMarshaller(table, metadata);
    }

    public <T> T execute(String scriptName, Map<String, ?> params) {
        return scriptManager.getCache().execute(scriptName, params);
    }

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

    @Override
    public void close() {

    }

}