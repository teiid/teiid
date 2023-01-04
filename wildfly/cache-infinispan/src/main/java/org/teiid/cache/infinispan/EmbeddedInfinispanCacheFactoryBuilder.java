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

import org.infinispan.commons.configuration.io.ConfigurationResourceResolver;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.TransactionConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.teiid.cache.CacheFactory;
import org.teiid.core.TeiidRuntimeException;

import javax.transaction.TransactionManager;
import java.io.IOException;
import java.io.InputStream;

/**
 * Needed to create the CacheFactory for embedded usage.
 */
public class EmbeddedInfinispanCacheFactoryBuilder {

    public static CacheFactory buildCacheFactory(String configFile, TransactionManager tm) {
        try {
            if (configFile == null) {
                configFile = "infinispan-config.xml"; // in classpath
            }
            InputStream inputStream = FileLookupFactory.newInstance().lookupFileStrict(configFile, Thread.currentThread().getContextClassLoader());
            ConfigurationBuilderHolder builderHolder = new ParserRegistry().parse(inputStream, ConfigurationResourceResolver.DEFAULT, MediaType.APPLICATION_XML);
            ConfigurationBuilder builder = builderHolder.getCurrentConfigurationBuilder();
            TransactionConfigurationBuilder transaction = builder.transaction();
            transaction.transactionManagerLookup(() -> tm);
            DefaultCacheManager cacheManager = new DefaultCacheManager(builderHolder, true);
            for (String cacheName : builderHolder.getNamedConfigurationBuilders().keySet()) {
                cacheManager.startCache(cacheName);
            }
            return new InfinispanCacheFactory(cacheManager, InfinispanCacheFactory.class.getClassLoader());
        } catch (IOException e) {
            throw new TeiidRuntimeException("Failed to initialize a Infinispan cache factory");
        }
    }

}
