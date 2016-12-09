package org.teiid.translator.object.testdata.person;

import java.util.Map;

import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;

public class PersonObjectConnection extends SimpleMapCacheConnection {

	public static ObjectConnection createConnection(Map<Object,Object> map) {
		CacheNameProxy proxy = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME);
		ObjectConnection conn = new PersonObjectConnection(map, PersonCacheSource.CLASS_REGISTRY, proxy);
		
		return conn;
	}
	
	public static ObjectConnection createConnection(Map<Object,Object> map, final String keyField) {
		CacheNameProxy proxy = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME);
		ObjectConnection conn = new PersonObjectConnection(map, PersonCacheSource.CLASS_REGISTRY, proxy){
			
			@Override
			public void setPkField(String keyfield) {
				super.setPkField(keyField);
			}
		};
		
		return conn;
	}

	public static ObjectConnection createConnection(Map<Object,Object> cache, Map<Object,Object> stagecache, Map<Object, Object> aliascache, CacheNameProxy proxy) {
		return new PersonObjectConnection(cache, stagecache, aliascache, PersonCacheSource.CLASS_REGISTRY, proxy);
	}

	public PersonObjectConnection(Map<Object,Object> map, ClassRegistry registry, CacheNameProxy proxy) {
		super(map, registry, proxy);
		setPkField("id");
		setCacheKeyClassType(int.class);
		this.setCacheClassType(Person.class);
	}
	
	public PersonObjectConnection(Map<Object, Object> cache, Map<Object, Object> stagecache, Map<Object, Object> aliascache, ClassRegistry registry, CacheNameProxy proxy) {
		super(cache, stagecache, aliascache, registry, proxy);
		setPkField("id");
		setCacheKeyClassType(int.class);
		this.setCacheClassType(Person.class);
	}	

}


