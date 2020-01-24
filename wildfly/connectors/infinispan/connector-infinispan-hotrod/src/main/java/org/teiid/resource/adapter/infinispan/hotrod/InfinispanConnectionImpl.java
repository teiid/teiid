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

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContext.MarshallerProvider;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanManagedConnectionFactory.InfinispanConnectionFactory;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;

public class InfinispanConnectionImpl extends BasicConnection implements InfinispanConnection {
    private RemoteCacheManager cacheManager;
    private String cacheName;

    private BasicCache<?, ?> defaultCache;
    private SerializationContext ctx;
    private SimpleMarshallerProvider marshallerProvider = new SimpleMarshallerProvider();
    private InfinispanConnectionFactory icf;
    private RemoteCacheManager scriptManager;
    private String cacheTemplate;

    public InfinispanConnectionImpl(RemoteCacheManager manager, RemoteCacheManager scriptManager, String cacheName,
            SerializationContext ctx, InfinispanConnectionFactory icf, String cacheTemplate) throws ResourceException {
        this.cacheManager = manager;
        this.cacheName = cacheName;
        this.ctx = ctx;
        this.ctx.registerMarshallerProvider(this.marshallerProvider);
        this.icf = icf;
        this.scriptManager = scriptManager;
        this.cacheTemplate = cacheTemplate;

        try {
            this.defaultCache = getCache(cacheName, true);
        } catch (Throwable t) {
            throw new ResourceException(t);
        }
    }

    @Override
    public void registerProtobufFile(ProtobufResource protobuf) throws TranslatorException {
        this.icf.registerProtobufFile(protobuf);
    }

    @Override
    public void close() throws ResourceException {
        // do not want to close on per cache basis
        // TODO: what needs to be done here?
        this.ctx.unregisterMarshallerProvider(this.marshallerProvider);
    }

    @Override
    public BasicCache getCache() throws TranslatorException {
        return defaultCache;
    }

    @Override
    public <K, V> BasicCache<K, V> getCache(String cacheName, boolean createIfNotExists) throws TranslatorException{
        RemoteCache<Object, Object> cache = cacheManager.getCache(cacheName);
        if (cache == null && createIfNotExists) {
            cacheManager.administration().createCache(cacheName, this.cacheTemplate);
            cache = cacheManager.getCache(cacheName);
        }
        return (BasicCache<K,V>)cache;
    }

    @Override
    public void registerMarshaller(BaseMarshaller<InfinispanDocument> marshaller) throws TranslatorException {
    	marshallerProvider.setMarsheller(marshaller);
    }

    @Override
    public void unRegisterMarshaller(BaseMarshaller<InfinispanDocument> marshaller) throws TranslatorException {
    	marshallerProvider.setMarsheller(null);
    }

    static class SimpleMarshallerProvider implements MarshallerProvider {

    	 private BaseMarshaller<?> marshaller;

        public void setMarsheller(BaseMarshaller<?> marshaller) throws TranslatorException {
        	if (this.marshaller == null) {
        		this.marshaller = marshaller;
        	} else {
        		if (marshaller == null) {
        			this.marshaller = null;
        		} else if (!this.marshaller.getTypeName().contentEquals(marshaller.getTypeName())) {
        			throw new TranslatorException("Already a marshaller present, connection can not be shared");
        		}
        	}
        }

        @Override
        public BaseMarshaller<?> getMarshaller(String typeName) {
        	return this.marshaller;
        }

        @Override
        public BaseMarshaller<?> getMarshaller(Class<?> javaClass) {
            if (javaClass.isAssignableFrom(InfinispanDocument.class)) {
            	return this.marshaller;
            }
            return null;
        }
    }

    @Override
    public <T> T execute(String scriptName, Map<String, ?> params) {
        return scriptManager.getCache().execute(scriptName, params);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void registerScript(String scriptName, String script) {
        RemoteCache cache = scriptManager.getCache("___script_cache");
        if (cache.get(scriptName) == null) {
            cache.put(scriptName, script);
        }
    }
}