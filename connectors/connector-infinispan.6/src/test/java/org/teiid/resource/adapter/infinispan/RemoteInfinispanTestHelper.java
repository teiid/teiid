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
package org.teiid.resource.adapter.infinispan;

import java.io.IOException;
import java.net.InetAddress;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.teiid.translator.object.testdata.TradesCacheSource;

/**
 */
@SuppressWarnings("nls")
public class RemoteInfinispanTestHelper {
	protected static final String TEST_CACHE_NAME = "test";
	protected static final String TRADE_CACHE_NAME = TradesCacheSource.TRADES_CACHE_NAME;

	protected static String CACHE_NAME = "NA"; //$NON-NLS-1$
	protected static final int PORT = 11311;
	protected static final int TIMEOUT = 0;
	protected static final String CONFIG_FILE = "src/test/resources/infinispan_remote_config.xml";

	private static HotRodServer server = null;
	private static int count = 0;
	private static DefaultCacheManager CACHEMANAGER = null;

	private static synchronized HotRodServer createServer() throws IOException {
		count++;
		if (server == null) {
			CACHEMANAGER = new DefaultCacheManager(CONFIG_FILE);

			CACHEMANAGER.start();
			// This doesn't work on IPv6, because the util assumes "127.0.0.1"
			// ...
			// server = HotRodTestingUtil.startHotRodServer(cacheManager, HOST,
			// PORT);

			String hostAddress = hostAddress();
			String hostPort = Integer.toString(hostPort());
			String timeoutStr = Integer.toString(TIMEOUT);

			HotRodServerConfigurationBuilder bldr = new HotRodServerConfigurationBuilder();
			bldr.proxyHost(hostAddress);
			bldr.proxyPort(PORT);
			HotRodServerConfiguration config = bldr.build();

			server = new HotRodServer();

			server.startInternal(config, CACHEMANAGER);

			server.cacheManager().startCaches(CACHE_NAME);

		}
		return server;
	}

	public static void loadCacheSimple() throws IOException {
		CACHE_NAME = TEST_CACHE_NAME;

		createServer();

		server.getCacheManager().getCache(CACHE_NAME)
				.put("1", new String("value1")); //$NON-NLS-1$  //$NON-NLS-2$
		server.getCacheManager().getCache(CACHE_NAME)
				.put("2", new String("value2")); //$NON-NLS-1$  //$NON-NLS-2$   	
	}

	public static void loadCacheTrades() throws IOException {
		CACHE_NAME = TRADE_CACHE_NAME;

		createServer();

		TradesCacheSource.loadCache(server.getCacheManager().getCache(
				CACHE_NAME));

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
				if (CACHEMANAGER != null) {
					CACHEMANAGER.stop();
				}
			} finally {
				CACHEMANAGER = null;
			}

			try {
				// System.out.println("Stopping HotRot Server at " +
				// hostAddress() + ":" + hostPort());
				if (server != null) {
					server.stop();
				}
			} finally {
				server = null;
			}

		}
	}
}
