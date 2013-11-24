package org.teiid.translator.object.infinispan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.translator.object.CacheContainerWrapper;

public abstract class InfinispanCacheWrapper extends CacheContainerWrapper {

		@Override
		public Object get(String cacheName, Object key) {
			Map cache = (Map) getCache(cacheName);
			return cache.get(key);
		}

		@Override
		public List<Object> getAll(String cacheName) {
			
			Map map = (Map) getCache(cacheName);

			Set<?> keys = map.keySet();
			List results = new ArrayList<Object>();
			for (Iterator<?> it = keys.iterator(); it.hasNext();) {
				Object v = map.get(it.next());
				results.add(v);

			}
			return results;
		}
		
}