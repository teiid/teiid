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
package org.teiid.infinispan.api;

import javax.resource.ResourceException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanConnectionImpl;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanManagedConnectionFactory;
import org.teiid.resource.spi.BasicConnectionFactory;

public class HotRodTestServer {
    private HotRodServer server;
    private DefaultCacheManager defaultCacheManager;
    private BasicConnectionFactory<InfinispanConnectionImpl> connectionFactory;

    public HotRodTestServer(int port) {
        ConfigurationBuilder c = new ConfigurationBuilder();
        GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder().nonClusteredDefault();
        GlobalConfiguration config = gc.build();
        this.defaultCacheManager = new DefaultCacheManager(config, c.build(config));
        this.defaultCacheManager.defineConfiguration("default", getConfigurationBuilder().build());
        this.defaultCacheManager.startCaches("default");
        this.defaultCacheManager.getCache();

        HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
        this.server = new HotRodServer();
        //InetSocketAddress addr = new InetSocketAddress(0);
        builder.host("127.0.0.1").port(port);
        server.start(builder.build(), this.defaultCacheManager);
    }

    protected ConfigurationBuilder getConfigurationBuilder() {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.indexing().index(Index.ALL).addProperty("default.directory_provider", "ram")
                .addProperty("lucene_version", "LUCENE_CURRENT");
        return builder;
    }

    public InfinispanConnection getConnection() throws ResourceException {
        if (connectionFactory == null) {
            InfinispanManagedConnectionFactory factory = new InfinispanManagedConnectionFactory();
            factory.setCacheName("default");
            factory.setRemoteServerList("127.0.0.1:"+server.getPort());
            connectionFactory = factory.createConnectionFactory();
        }
        return this.connectionFactory.getConnection();
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
