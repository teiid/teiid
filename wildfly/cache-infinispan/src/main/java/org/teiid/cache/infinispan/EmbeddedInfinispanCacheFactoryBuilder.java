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

package org.teiid.cache.infinispan;

import java.io.IOException;

import javax.transaction.TransactionManager;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.manager.DefaultCacheManager;
import org.teiid.cache.CacheFactory;
import org.teiid.core.TeiidRuntimeException;

/**
 * Needed to create the CacheFactory for embedded usage.
 */
public class EmbeddedInfinispanCacheFactoryBuilder {

    public static CacheFactory buildCacheFactory(String configFile, TransactionManager tm) {
        try {
            if (configFile == null) {
                configFile = "infinispan-config.xml"; // in classpath
            }
            DefaultCacheManager cacheManager = new DefaultCacheManager(configFile, true);
            for(String cacheName:cacheManager.getCacheNames()) {
                if (tm != null) {
                    cacheManager.getCacheConfiguration(cacheName).transaction().transactionManagerLookup(new TransactionManagerLookup() {
                        @Override
                        public TransactionManager getTransactionManager() throws Exception {
                            return tm;
                        }
                    });
                }
                cacheManager.startCache(cacheName);
            }
            return new InfinispanCacheFactory(cacheManager, InfinispanCacheFactory.class.getClassLoader());
        } catch (IOException e) {
            throw new TeiidRuntimeException("Failed to initialize a Infinispan cache factory");
        }
    }

}
