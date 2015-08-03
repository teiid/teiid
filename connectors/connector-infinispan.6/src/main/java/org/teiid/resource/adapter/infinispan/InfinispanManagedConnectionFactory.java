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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.teiid.resource.adapter.infinispan.base.AbstractInfinispanManagedConnectionFactory;
import org.teiid.translator.object.CacheContainerWrapper;

public class InfinispanManagedConnectionFactory extends
		AbstractInfinispanManagedConnectionFactory {

	@Override
	protected CacheContainerWrapper createRemoteCache(Properties props,
			ClassLoader classLoader) throws ResourceException {
		RemoteCacheManager remoteCacheManager;
		try {
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.marshaller(new PojoMarshaller(classLoader));
			cb.withProperties(props);
			remoteCacheManager = new RemoteCacheManager(cb.build(), true);
		} catch (Exception err) {
			throw new ResourceException(err);
		}

		return new RemoteCacheWrapper(remoteCacheManager);

	}

}

class RemoteCacheWrapper extends CacheContainerWrapper {
	RemoteCacheManager rcm = null;

	public RemoteCacheWrapper(RemoteCacheManager remoteCacheManager) {
		super();
		rcm = remoteCacheManager;
	}

	@Override
	public List<Object> getAll(String cacheName) {
		RemoteCache cache = getCache(cacheName);
		Map<?, ?> map = (Map) cache.getBulk();
		Set<?> keys = map.keySet();
		List results = new ArrayList<Object>();
		for (Iterator<?> it = keys.iterator(); it.hasNext();) {
			Object v = map.get(it.next());
			results.add(v);
		}
		return results;
	}

	@Override
	public RemoteCache getCache(String cacheName) {

		if (cacheName == null) {
			return rcm.getCache();
		}
		return rcm.getCache(cacheName);

	}

	// added to enable unit test to close the container
	public void cleanUp() {
		rcm.stop();
	}
}
