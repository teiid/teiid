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

public interface InfinispanConfiguration {

    /**
     * See the inifinspan docs for all transation modes including NONE, NON_XA, NON_DURABLE_XA, and FULL_XA
     */
    String getTransactionMode();
    /**
     * A ; separated list of host:port servers
     */
    String getRemoteServerList();
    /**
     * The name of the cache for use by this source.
     */
    String getCacheName();
    /**
     * The name of the cache template, which must already be registered, for impliciting creating the cache
     * if it does not exist.
     */
    String getCacheTemplate();

    /**
     * The sasl mechanism.  May be left at the default of null when using simple username/password
     * authenticataion.  See the infinispan docs for all values, including PLAIN, DIGEST-MD5, GSSAPI, and EXTERNAL.
     * <br>When set to EXTERNAL the key and trust store properties are required.
     */
    String getSaslMechanism();
    String getKeyStoreFileName();
    String getKeyStorePassword();
    String getTrustStoreFileName();
    String getTrustStorePassword();
    String getUsername();
    String getPassword();
    /**
     * The authentication realm name, typicallly defaults to default
     */
    String getAuthenticationRealm();
    /**
     * The authentication server name, typically defaults to infinispan
     */
    String getAuthenticationServerName();
}
