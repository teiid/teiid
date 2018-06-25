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

import org.infinispan.commons.api.BasicCache;
import org.infinispan.protostream.BaseMarshaller;
import org.teiid.translator.TranslatorException;

public interface InfinispanConnection {

    <K, V> BasicCache<K, V> getCache(String cacheName, boolean createIfNotExists) throws TranslatorException;

    <K, V> BasicCache<K, V> getCache() throws TranslatorException;

    void registerProtobufFile(ProtobufResource protobuf) throws TranslatorException;

    void registerMarshaller(BaseMarshaller<InfinispanDocument> marshller) throws TranslatorException;

    void unRegisterMarshaller(BaseMarshaller<InfinispanDocument> marshller) throws TranslatorException;

    void registerScript(String scriptName, String script);
    
    <T> T execute(String scriptName, Map<String, ?> params);
}
