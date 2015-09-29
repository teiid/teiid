package org.teiid.translator.object.testdata.person;

import java.util.Map;

import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;

public class PersonObjectConnection extends SimpleMapCacheConnection {

	public static ObjectConnection createConnection(Map<Object,Object> map) {
		return new PersonObjectConnection(map, PersonCacheSource.CLASS_REGISTRY);
	}

	public PersonObjectConnection(Map<Object,Object> map, ClassRegistry registry) {
		super(map, registry);
		setPkField("id");
		setCacheKeyClassType(int.class);
		this.setCacheName(PersonCacheSource.PERSON_CACHE_NAME);
		this.setCacheClassType(Person.class);
	}

}


