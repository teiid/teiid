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

import javax.resource.ResourceException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanManagedConnectionFactory;
import org.teiid.resource.spi.BasicConnectionFactory;

public class HotRodTestServer {
    private HotRodServer server;
    private DefaultCacheManager defaultCacheManager;
    private BasicConnectionFactory<InfinispanManagedConnectionFactory.InfinispanResourceConnection> connectionFactory;

    public HotRodTestServer(int port) {
        ConfigurationBuilder c = getConfigurationBuilder();
                //new ConfigurationBuilder();
        GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder().nonClusteredDefault();
        gc.defaultCacheName("default");

        GlobalConfiguration config = gc.build();
        this.defaultCacheManager = new DefaultCacheManager(config, c.build(config));
        this.defaultCacheManager.defineConfiguration("bar", getConfigurationBuilder().build());

        this.defaultCacheManager.defineConfiguration("foo", getConfigurationBuilder().build());

        this.defaultCacheManager.startCaches("bar", "foo", ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
        this.defaultCacheManager.getCache();

        HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
        this.server = new HotRodServer();
        //InetSocketAddress addr = new InetSocketAddress(0);
        builder.host("127.0.0.1").port(port);
        server.start(builder.build(), this.defaultCacheManager);
    }

    protected ConfigurationBuilder getConfigurationBuilder() {
        ConfigurationBuilder builder = new ConfigurationBuilder();
//        builder.indexing().index(Index.ALL).addProperty("default.directory_provider", "ram")
//                .addProperty("lucene_version", "LUCENE_CURRENT");
        return builder;
    }

    public InfinispanConnection getConnection(String cacheName) throws ResourceException {
        if (connectionFactory == null) {
            InfinispanManagedConnectionFactory factory = new InfinispanManagedConnectionFactory();
            factory.setCacheName(cacheName);
            factory.setRemoteServerList("127.0.0.1:"+server.getPort());
            connectionFactory = factory.createConnectionFactory();
        }
        return this.connectionFactory.getConnection();
    }

    public InfinispanConnection getConnection() throws ResourceException {
        return getConnection("default");
    }

    public void stop() {
        if (server != null) {
            this.defaultCacheManager.stop();
            this.server.stop();
        }
        this.defaultCacheManager = null;
        this.server = null;
    }
}
