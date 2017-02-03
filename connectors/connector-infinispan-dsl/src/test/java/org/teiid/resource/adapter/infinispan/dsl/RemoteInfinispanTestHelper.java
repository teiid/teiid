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
package org.teiid.resource.adapter.infinispan.dsl;

import java.io.IOException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person;
import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;


/**
 */
@SuppressWarnings("nls")
public class RemoteInfinispanTestHelper {
	protected static final String TEST_CACHE_NAME = "test";
	protected static final String PERSON_CACHE_NAME = PersonCacheSource.PERSON_CACHE_NAME;
	protected static final Class<?> PERSON_CLASS = Person.class;
	protected static final String PKEY_COLUMN = "id";
	
	protected static final int PORT = 11312;
	protected static final int TIMEOUT = 0;

	private static HotRodServer server = null;
	private static int count = 0;
	private static DefaultCacheManager CACHEMANAGER = null;

	private static synchronized HotRodServer createServer()  {
		if (server == null) {
			try {
								
				CACHEMANAGER = new DefaultCacheManager(new ConfigurationBuilder()
		            .eviction()
		            .build());
	
				CACHEMANAGER.start();
				
				CACHEMANAGER.startCache(PERSON_CACHE_NAME);
				CACHEMANAGER.startCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
								
				PersonCacheSource.loadCache(CACHEMANAGER.getCache(PERSON_CACHE_NAME));
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			String hostAddress = hostAddress();

			HotRodServerConfigurationBuilder bldr = new HotRodServerConfigurationBuilder();
			bldr.host(hostAddress);
			bldr.port(PORT);
			
			HotRodServerConfiguration config = bldr.create();

			server = new HotRodServer();

			server.startInternal(config, CACHEMANAGER);
			server.cacheManager().startCaches(PERSON_CACHE_NAME);

		}
		count++;
		return server;
	}
	
	public static void startServer() throws IOException {
		createServer();
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
			return "localhost";
	}

	public static synchronized void releaseServer() {
		count--;
		if (count == 0) {
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
