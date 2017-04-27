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