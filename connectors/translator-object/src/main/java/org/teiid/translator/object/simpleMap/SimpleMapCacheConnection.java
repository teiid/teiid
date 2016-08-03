package org.teiid.translator.object.simpleMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.DDLHandler;
import org.teiid.translator.object.SearchType;

public class SimpleMapCacheConnection implements ObjectConnection {
	private Map<String, Map<Object,Object>> mapCaches = new HashMap<String, Map<Object, Object>>(3);
	private ClassRegistry registry;
	private String pkField;
	private Class<?> cacheKeyClassType;
	private String cacheName = "SimpleCache";
	private Class<?> cacheClassType;
	private CacheNameProxy proxy;

	public SimpleMapCacheConnection(Map<Object, Object> cache, ClassRegistry registry, CacheNameProxy proxy){
		mapCaches.put(proxy.getPrimaryCacheKey(), cache);
		this.cacheName = proxy.getPrimaryCacheKey();
		this.registry = registry;
		this.proxy = proxy;
	}

	public SimpleMapCacheConnection(Map<Object, Object> cache, Map<Object, Object> stagecache, Map<Object, Object> aliascache, ClassRegistry registry, CacheNameProxy proxy){
		
		this.cacheName = proxy.getPrimaryCacheKey();
		
		mapCaches.put(proxy.getPrimaryCacheKey(), cache);
		mapCaches.put(proxy.getStageCacheKey(), stagecache);
		mapCaches.put(proxy.getAliasCacheName(), aliascache);
		
		this.registry = registry;
		this.proxy = proxy;
	}
	
	@Override
	public String getVersion()  {
		return "";
	}

	@Override 
	public  Map<Object, Object> getCache() throws TranslatorException {
		return getCache(getCacheName());
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache(java.lang.String)
	 */
	@Override
	public Map<Object, Object> getCache(String cacheName) throws TranslatorException {

		String cn = null;
		if (cacheName.equals(proxy.getAliasCacheName())) {
			cn = cacheName;
		} else {
			cn = proxy.getCacheName(cacheName);
		}
		
		Map<Object, Object> cache = mapCaches.get(cn);
		if (cache == null) {
			String keymsg = proxy.getPrimaryCacheKey() + "|" +  (proxy.getStageCacheKey() != null ? proxy.getStageCacheKey() : "N/A") + "|" + (proxy.getAliasCacheName() != null ? proxy.getAliasCacheName() : "N/A") ;
			throw new TranslatorException("Cache " + cacheName + " is not defined, must be: " + keymsg);
		}
		return cache;
	}	
	
	
	@Override
	public void cleanUp() {
		mapCaches.clear();
	}

	/**
	 * @return registry
	 */
	@Override
	public ClassRegistry getClassRegistry() {
		return registry;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#isAlive()
	 */
	@Override
	public boolean isAlive() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getPkField()
	 */
	@Override
	public String getPkField() {
		return pkField;
	}
	
	public void setPkField(String pk) {
		this.pkField = pk;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheKeyClassType()
	 */
	@Override
	public Class<?> getCacheKeyClassType() {
		return cacheKeyClassType;
	}
	
	public void setCacheKeyClassType(Class<?> keyClassType) {
		cacheKeyClassType = keyClassType;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheName()
	 */
	@Override
	public String getCacheName()  {
		return cacheName;
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheClassType()
	 */
	@Override
	public Class<?> getCacheClassType()  {
		return cacheClassType;
	}
	
	public void setCacheClassType(Class<?> classType) {
		this.cacheClassType = classType;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#add(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void add(Object key, Object value) throws TranslatorException {
		this.getCache().put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key) throws TranslatorException {
		return this.getCache().remove(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#update(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void update(Object key, Object value) throws TranslatorException {
		this.getCache().put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#get(java.lang.Object)
	 */
	@Override
	public Object get(Object key)  throws TranslatorException {
		return this.getCache().get(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getAll()
	 */
	@Override
	public Collection<Object> getAll()  throws TranslatorException {
		Collection<Object> objs = new ArrayList<Object>();
		Map<Object, Object> c = getCache();
		for (Object k : c.keySet()) {
			objs.add(c.get(k));
		}
		return objs;
	}
	
	/**
	 * Implement @link DDLHandler if the translator supports materialization.
	 * The default implementation expects to use a second cache to manage the cache alias names
	 * in order to support materialization into a staging location.
	 * @return DDLHandler
	 */
	@Override
	public DDLHandler getDDLHandler() {
		return proxy.getDDLHandler();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#clearCache(java.lang.String)
	 */
	@Override
	public void clearCache(String cacheName) throws TranslatorException {
		Map<Object, Object> c = getCache(cacheName);
		c.clear();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getSearchType()
	 */
	@Override
	public SearchType getSearchType() {
		return new SearchByKey(this);
	}	
	
	
	
}


