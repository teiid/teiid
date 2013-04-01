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
package org.teiid.translator.object.infinispan;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.Main;
import org.infinispan.server.hotrod.HotRodServer;
import org.teiid.translator.object.util.TradesCacheSource;

/**
 */
public class RemoteInfinispanTestHelper {
    protected static final int PORT = 11311;
    protected static final int TIMEOUT = 0;
    private static HotRodServer server = null;
    private static int count = 0;
    private static  DefaultCacheManager CACHEMANAGER = null;

    public static synchronized HotRodServer createServer() {
        count++;
        if (server == null) {
        	Configuration c = new ConfigurationBuilder().clustering().cacheMode(CacheMode.REPL_SYNC).eviction().maxEntries(7).build();
        	
        	CACHEMANAGER = new DefaultCacheManager(
                    new GlobalConfigurationBuilder().transport().defaultTransport().build(),
                    c);
        	CACHEMANAGER.start();
            // This doesn't work on IPv6, because the util assumes "127.0.0.1" ...
            // server = HotRodTestingUtil.startHotRodServer(cacheManager, HOST, PORT);
        	
        	
        	CACHEMANAGER.defineConfiguration(TradesCacheSource.TRADES_CACHE_NAME, c);
        	
            server = new HotRodServer();
            String hostAddress = hostAddress();
            String hostPort = Integer.toString(hostPort());
            String timeoutStr = Integer.toString(TIMEOUT);
            Properties props = new Properties();
            props.setProperty(Main.PROP_KEY_HOST(), hostAddress);
            props.setProperty(Main.PROP_KEY_PORT(), hostPort);
            props.setProperty(Main.PROP_KEY_IDLE_TIMEOUT(), timeoutStr);
            props.setProperty(Main.PROP_KEY_PROXY_HOST(), hostAddress);
            props.setProperty(Main.PROP_KEY_PROXY_PORT(), hostPort);
            // System.out.println("Starting HotRot Server at " + hostAddress + ":" + hostPort);
            server.start(props, CACHEMANAGER);
            
            server.cacheManager().startCaches(TradesCacheSource.TRADES_CACHE_NAME);
            
            TradesCacheSource.loadCache(server.getCacheManager().getCache(TradesCacheSource.TRADES_CACHE_NAME));
 
        }
        return server;
    }
    
    public static DefaultCacheManager getCacheManager() {
    	return CACHEMANAGER;
    }

    public static int hostPort() {
        return PORT;
    }

    /**
     * Return the IP address of this host, in either IPv4 or IPv6 format.
     * 
     * @return the IP address as a string
     */
    public static String hostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized void releaseServer() {
        --count;
        if (count <= 0) {
            try {
                // System.out.println("Stopping HotRot Server at " + hostAddress() + ":" + hostPort());
                server.stop();
                getCacheManager().stop();
            } finally {
                server = null;
            }
        }
    }
}
