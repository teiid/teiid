package org.teiid.translator.object;

import java.util.List;

/**
 * The CacheContainerWrapper serves to hide the implementation details of the actual cache, because not all caches extend a common interface (i.e, Map).  An implementation
 * will map the required behavior to that being defined by the abstract methods.
 * 
 * @author vhalbert
 *
 */
public abstract class CacheContainerWrapper
{
	
	public CacheContainerWrapper() {
	}
	
	/**
	 * Call to obtain an object from the cache based on the specified key
	 * @param cacheName
	 * @param key to use to get the object from the cache
	 * @return Object
	 */
	public abstract Object get(String cacheName, Object key);
	
	/**
	 * Call to obtain all the objects from the cache
	 * @param cacheName
	 * @return List of all the objects in the cache
	 */
	public abstract List<Object> getAll(String cacheName);
	
	/**
	 * Call to obtain the cache object
	 * @param cacheName
	 * @return Object cache
	 */
	public abstract Object getCache(String cacheName);  

}