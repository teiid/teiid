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

package org.teiid.jboss;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("nls")
enum Element {
    // must be first
    UNKNOWN(null),

    // VM wide elements
    ASYNC_THREAD_POOL_ELEMENT("async-thread-pool", "async-thread-pool"),
    THREAD_COUNT_ATTRIBUTE("max-thread-count", "async-thread-pool-max-thread-count"),

    ALLOW_ENV_FUNCTION_ELEMENT("allow-env-function", "allow-env-function"),

    MAX_THREADS_ELEMENT("max-threads", "max-threads"),
    MAX_ACTIVE_PLANS_ELEMENT("max-active-plans", "max-active-plans"),
    USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT("thread-count-for-source-concurrency", "thread-count-for-source-concurrency"),
    TIME_SLICE_IN_MILLI_ELEMENT("time-slice-in-milliseconds", "time-slice-in-milliseconds"),
    MAX_ROWS_FETCH_SIZE_ELEMENT("max-row-fetch-size", "max-row-fetch-size"),
    LOB_CHUNK_SIZE_IN_KB_ELEMENT("lob-chunk-size-in-kb", "lob-chunk-size-in-kb"),
    QUERY_THRESHOLD_IN_SECS_ELEMENT("query-threshold-in-seconds", "query-threshold-in-seconds"),
    MAX_SOURCE_ROWS_ELEMENT("max-source-rows-allowed", "max-source-rows-allowed"),
    EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT("exception-on-max-source-rows", "exception-on-max-source-rows"),
    DETECTING_CHANGE_EVENTS_ELEMENT("detect-change-events", "detect-change-events"),
    QUERY_TIMEOUT("query-timeout", "query-timeout"),
    WORKMANAGER("workmanager", "workmanager"),

    POLICY_DECIDER_MODULE_ELEMENT("policy-decider-module", "policy-decider-module"),
    DATA_ROLES_REQUIRED_ELEMENT("data-roles-required", "data-roles-required"),
    AUTHORIZATION_VALIDATOR_MODULE_ELEMENT("authorization-validator-module", "authorization-validator-module"),
    PREPARSER_MODULE_ELEMENT("preparser-module", "preparser-module"),

    // buffer manager
    @Deprecated
    BUFFER_SERVICE_ELEMENT("buffer-service"),
    @Deprecated
    USE_DISK_ATTRIBUTE("use-disk", "buffer-service-use-disk"),
    @Deprecated
    PROCESSOR_BATCH_SIZE_ATTRIBUTE("processor-batch-size", "buffer-service-processor-batch-size"),
    @Deprecated
    MAX_PROCESSING_KB_ATTRIBUTE("max-processing-kb", "buffer-service-max-processing-kb"),
    @Deprecated
    MAX_RESERVED_KB_ATTRIBUTE("max-reserve-kb", "buffer-service-max-reserve-kb"),
    @Deprecated
    MAX_FILE_SIZE_ATTRIBUTE("max-file-size", "buffer-service-max-file-size"),
    @Deprecated
    MAX_BUFFER_SPACE_ATTRIBUTE("max-buffer-space", "buffer-service-max-buffer-space"),
    @Deprecated
    MAX_OPEN_FILES_ATTRIBUTE("max-open-files", "buffer-service-max-open-files"),
    @Deprecated
    MEMORY_BUFFER_SPACE_ATTRIBUTE("memory-buffer-space", "buffer-service-memory-buffer-space"),
    @Deprecated
    MEMORY_BUFFER_OFFHEAP_ATTRIBUTE("memory-buffer-off-heap", "buffer-service-memory-buffer-off-heap"),
    @Deprecated
    MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE("max-storage-object-size", "buffer-service-max-storage-object-size"),
    @Deprecated
    INLINE_LOBS("inline-lobs", "buffer-service-inline-lobs"),
    @Deprecated
    ENCRYPT_FILES_ATTRIBUTE("encrypt-files", "buffer-service-encrypt-files"),

    BUFFER_MANAGER_ELEMENT("buffer-manager"),
    BUFFER_MANAGER_PROCESSOR_BATCH_SIZE_ATTRIBUTE("processor-batch-size", "buffer-manager-processor-batch-size"),
    BUFFER_MANAGER_INLINE_LOBS("inline-lobs", "buffer-manager-inline-lobs"),
    BUFFER_MANAGER_MAX_PROCESSING_KB_ATTRIBUTE("heap-max-processing-kb", "buffer-manager-heap-max-processing-kb"),
    BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE("heap-max-reserve-mb", "buffer-manager-heap-max-reserve-mb"),
    BUFFER_MANAGER_USE_DISK_ATTRIBUTE("storage-enabled", "buffer-manager-storage-enabled"),
    BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE("storage-max-object-size-kb", "buffer-manager-storage-max-object-size-kb"),
    BUFFER_MANAGER_MEMORY_BUFFER_SPACE_ATTRIBUTE("fixed-memory-space-mb", "buffer-manager-fixed-memory-space-mb"),
    BUFFER_MANAGER_MEMORY_BUFFER_OFFHEAP_ATTRIBUTE("fixed-memory-off-heap", "buffer-manager-fixed-memory-off-heap"),
    BUFFER_MANAGER_MAX_FILE_SIZE_ATTRIBUTE("disk-max-file-size-mb", "buffer-manager-disk-max-file-size-mb"),
    BUFFER_MANAGER_MAX_BUFFER_SPACE_ATTRIBUTE("disk-max-space-mb", "buffer-manager-disk-max-space-mb"),
    BUFFER_MANAGER_MAX_OPEN_FILES_ATTRIBUTE("disk-max-open-files", "buffer-manager-disk-max-open-files"),
    BUFFER_MANAGER_ENCRYPT_FILES_ATTRIBUTE("disk-encrypt-files", "buffer-manager-disk-encrypt-files"),

    //prepared-plan-cache-config
    PREPAREDPLAN_CACHE_ELEMENT("preparedplan-cache"),
    PPC_ENABLE_ATTRIBUTE("enable", "preparedplan-cache-enable"),
    PPC_NAME_ATTRIBUTE("name", "preparedplan-cache-name"),
    PPC_CONTAINER_NAME_ELEMENT("infinispan-container", "preparedplan-cache-infinispan-container"),

    // Object Replicator
    DISTRIBUTED_CACHE("distributed-cache"),
    DC_STACK_ATTRIBUTE("jgroups-stack", "distributed-cache-jgroups-stack"),

    // Result set cache
    RESULTSET_CACHE_ELEMENT("resultset-cache"),
    RSC_ENABLE_ATTRIBUTE("enable", "resultset-cache-enable"),
    RSC_NAME_ATTRIBUTE("name", "resultset-cache-name"),
    RSC_CONTAINER_NAME_ATTRIBUTE("infinispan-container", "resultset-cache-infinispan-container"),
    RSC_MAX_STALENESS_ATTRIBUTE("max-staleness", "resultset-cache-max-staleness"),

    //transport
    TRANSPORT_ELEMENT("transport"),
    TRANSPORT_PROTOCOL_ATTRIBUTE("protocol", "protocol"),
    TRANSPORT_NAME_ATTRIBUTE("name", "name"),
    TRANSPORT_SOCKET_BINDING_ATTRIBUTE("socket-binding", "socket-binding"),
    TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE("max-socket-threads", "max-socket-threads"),
    TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE("input-buffer-size", "input-buffer-size"),
    TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE("output-buffer-size", "output-buffer-size"),

    AUTHENTICATION_ELEMENT("authentication"),
    AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE("security-domain", "authentication-security-domain"),
    AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE("max-sessions-allowed", "authentication-max-sessions-allowed"),
    AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE("sessions-expiration-timelimit", "authentication-sessions-expiration-timelimit"),
    AUTHENTICATION_TYPE_ATTRIBUTE("type", "authentication-type"),
    AUTHENTICATION_TRUST_ALL_LOCAL_ATTRIBUTE("trust-all-local", "authentication-trust-all-local"),

    PG_ELEMENT("pg"), //$NON-NLS-1$
    PG_MAX_LOB_SIZE_ALLOWED_ELEMENT("max-lob-size-in-bytes", "pg-max-lob-size-in-bytes"), //$NON-NLS-1$ //$NON-NLS-2$

    SSL_ELEMENT("ssl"),
    SSL_MODE_ATTRIBUTE("mode", "ssl-mode"),
    SSL_AUTH_MODE_ATTRIBUTE("authentication-mode", "ssl-authentication-mode"),
    SSL_SSL_PROTOCOL_ATTRIBUTE("ssl-protocol", "ssl-ssl-protocol"),
    SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE("keymanagement-algorithm", "ssl-keymanagement-algorithm"),
    SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE("enabled-cipher-suites", "ssl-enabled-cipher-suites"),
    SSL_KETSTORE_ELEMENT("keystore"),
    SSL_KETSTORE_NAME_ATTRIBUTE("name", "keystore-name"),
    SSL_KETSTORE_ALIAS_ATTRIBUTE("key-alias", "keystore-key-alias"),
    SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE("key-password", "keystore-key-password"),
    SSL_KETSTORE_PASSWORD_ATTRIBUTE("password", "keystore-password"),
    SSL_KETSTORE_TYPE_ATTRIBUTE("type", "keystore-type"),
    SSL_TRUSTSTORE_ELEMENT("truststore"),
    SSL_TRUSTSTORE_NAME_ATTRIBUTE("name", "truststore-name"),
    SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE("password", "truststore-password"),
    SSL_TRUSTSTORE_CHECK_EXIRIED_ATTRIBUTE("check-expired", "truststore-check-expired"),

    // Translator
    TRANSLATOR_ELEMENT("translator"),
    TRANSLATOR_NAME_ATTRIBUTE("name", "name"),
    TRANSLATOR_MODULE_ATTRIBUTE("module", "module"),
    TRANSLATOR_SLOT_ATTRIBUTE("slot", "slot");

    private final String xmlName;
    private final String modelName;

    private Element(String xmlName) {
        this.xmlName = xmlName;
        this.modelName = xmlName;
    }

    Element(final String xmlName, final String modelName) {
        this.xmlName = xmlName;
        this.modelName = modelName;
    }

    public String getXMLName() {
        return xmlName;
    }

    public String getLocalName() {
        return xmlName;
    }
    public String getModelName() {
        return this.modelName;
    }

    private static final Map<String, Element> elements;

    static {
        final Map<String, Element> map = new HashMap<String, Element>();
        for (Element element : values()) {
            final String name = element.getModelName();
            if (name != null) map.put(name, element);
        }
        elements = map;
    }

    public static Element forName(String localName, Element parentNode) {
        String modelName = parentNode.getModelName()+"-"+localName;
        final Element element = elements.get(modelName);
        return element == null ? UNKNOWN : element;
    }

    public static Element forName(String localName) {
        final Element element = elements.get(localName);
        return element == null ? UNKNOWN : element;
    }
}

