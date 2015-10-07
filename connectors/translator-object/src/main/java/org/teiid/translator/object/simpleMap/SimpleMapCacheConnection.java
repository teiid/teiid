package org.teiid.translator.object.simpleMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;

public class SimpleMapCacheConnection implements ObjectConnection {
	private Map<Object, Object> cache;
	private ClassRegistry registry;
	private String pkField;
	private Class<?> cacheKeyClassType;
	private String cacheName = "SimpleCache";
	private Class<?> cacheClassType;

	public SimpleMapCacheConnection(Map<Object, Object> cache, ClassRegistry registry){
		this.cache = cache;
		this.registry = registry;
	}
	
	@Override 
	public  Map<Object, Object> getCache() {
		return cache;
	}
	
	
	@Override
	public void cleanUp() {
		cache = null;
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
	
	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
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
	public void add(Object key, Object value) {
		this.getCache().put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key){
		return this.getCache().remove(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#update(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void update(Object key, Object value) {
		this.getCache().put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#get(java.lang.Object)
	 */
	@Override
	public Object get(Object key)  {
		return this.getCache().get(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getAll()
	 */
	@Override
	public Collection<Object> getAll()  {
		Collection<Object> objs = new ArrayList<Object>();
		Map<Object, Object> c = getCache();
		for (Object k : c.keySet()) {
			objs.add(c.get(k));
		}
		return objs;
	}
	
}


