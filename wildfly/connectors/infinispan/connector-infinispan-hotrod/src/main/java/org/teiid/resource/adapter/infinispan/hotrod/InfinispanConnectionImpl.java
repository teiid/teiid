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

import org.infinispan.commons.api.BasicCache;
import org.teiid.infinispan.api.BaseInfinispanConnection;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.ExecutionFactory.TransactionSupport;
import org.teiid.translator.TranslatorException;

public class InfinispanConnectionImpl extends BasicConnection implements InfinispanConnection {
    private BaseInfinispanConnection bic;

    public InfinispanConnectionImpl(BaseInfinispanConnection bic) {
        this.bic = bic;
    }

    @Override
    public void close() throws ResourceException {
        bic.close();
    }

    @Override
    public <K, V> BasicCache<K, V> getCache(String cacheName)
            throws TranslatorException {
        return bic.getCache(cacheName);
    }

    @Override
    public <K, V> BasicCache<K, V> getCache() throws TranslatorException {
        return bic.getCache();
    }

    @Override
    public void registerProtobufFile(ProtobufResource protobuf)
            throws TranslatorException {
        bic.registerProtobufFile(protobuf);
    }

    @Override
    public void registerMarshaller(Table table, RuntimeMetadata metadata)
            throws TranslatorException {
        bic.registerMarshaller(table, metadata);
    }

    @Override
    public <T> T execute(String scriptName, Map<String, ?> params) {
        return bic.execute(scriptName, params);
    }

    @Override
    public TransactionSupport getTransactionSupport() {
        return bic.getTransactionSupport();
    }

}