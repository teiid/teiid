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


import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.BasicCacheContainer;
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
    private ThreadAwareMarshallerProvider marshallerProvider = new ThreadAwareMarshallerProvider();
    private InfinispanConnectionFactory icf;

    public InfinispanConnectionImpl(RemoteCacheManager manager, String cacheName, SerializationContext ctx,
            InfinispanConnectionFactory icf) throws ResourceException {
        this.cacheManager = manager;
        this.cacheName = cacheName;
        this.ctx = ctx;
        this.ctx.registerMarshallerProvider(this.marshallerProvider);
        this.icf = icf;
        try {
            this.defaultCache = this.cacheManager.getCache(this.cacheName);
        } catch (Throwable t) {
            throw new ResourceException(t);
        }
    }

    @Override
    public void registerProtobufFile(ProtobufResource protobuf) throws TranslatorException {
        this.icf.registerProtobufFile(protobuf);
    }

    @Override
    public BasicCacheContainer getCacheFactory() throws TranslatorException {
        return this.cacheManager;
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
    public void registerMarshaller(BaseMarshaller<InfinispanDocument> marshaller) throws TranslatorException {
        ThreadAwareMarshallerProvider.setMarsheller(marshaller);
    }

    @Override
    public void unRegisterMarshaller(BaseMarshaller<InfinispanDocument> marshaller) throws TranslatorException {
        ThreadAwareMarshallerProvider.setMarsheller(null);
    }

    /**
     * The reason for thread aware marshaller is due to fact the serialization context is JVM wide, so if some other
     * connection is also trying to register a marshaller for same object, they should not conflict.
     */
    static class ThreadAwareMarshallerProvider implements MarshallerProvider {

        private static ThreadLocal<BaseMarshaller<?>> context = new ThreadLocal<BaseMarshaller<?>>() {
            @Override
            protected BaseMarshaller<?> initialValue() {
                return null;
            }
        };

        public static void setMarsheller(BaseMarshaller<?> marshaller) {
            context.set(marshaller);
        }

        @Override
        public BaseMarshaller<?> getMarshaller(String typeName) {
            BaseMarshaller<?> m = context.get();
            if (m != null && typeName.equals(m.getTypeName())) {
                return context.get();
            }
            return null;
        }

        @Override
        public BaseMarshaller<?> getMarshaller(Class<?> javaClass) {
            BaseMarshaller<?> m = context.get();
            if (m != null && javaClass.isAssignableFrom(InfinispanDocument.class)) {
                return context.get();
            }
            return null;
        }
    }
}