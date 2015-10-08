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
package org.teiid.translator.object.testdata.person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;

/**
 * Sample cache of objects
 * 
 *
 */
@SuppressWarnings({ "nls" })
public class PersonCacheSource extends HashMap <Object, Object> {
	
	/**
	 */
	private static final long serialVersionUID = 1L;

	public static final String PERSON_CACHE_NAME = "PersonsCache";
	
	public static final int NUMPERSONS = 10;
	public static final int NUMPHONES = 2;
	
	public static ClassRegistry CLASS_REGISTRY = new ClassRegistry();

	
	static {
		try {
			CLASS_REGISTRY.registerClass(Person.class);
			CLASS_REGISTRY.registerClass(PhoneNumber.class);
			CLASS_REGISTRY.registerClass(PhoneType.class);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	
	public static ObjectConnection createConnection() {
		final Map <Object, Object> objects = PersonCacheSource.loadCache();

		ObjectConnection toc = PersonObjectConnection.createConnection(objects);
		return toc;

	}
	
	public static void loadCache(Map<Object, Object> cache) {
		PhoneType[] types = new PhoneType[] {PhoneType.HOME, PhoneType.MOBILE, PhoneType.WORK};
		int t = 0;
		
		for (int i = 1; i <= NUMPERSONS; i++) {
			
			List<PhoneNumber> pns = new ArrayList<PhoneNumber>();
			for (int j = 1; j <= NUMPHONES; j++) {
				PhoneNumber pn = new PhoneNumber("(111)222-345" + j, types[t++]);
				if (t > 2) t = 0;				
				pns.add(pn);
			}
			
			Person p = new Person();
			p.setId(i);
			p.setName("Person " + i);
			p.setPhones(pns);
				
			cache.put(i, p);

		}
	}

	public static Map<Object, Object>  loadCache() {
		PersonCacheSource tcs = new PersonCacheSource();
		PersonCacheSource.loadCache(tcs);
		return tcs;
	}
	
	public List<Object> get(int key) {
		List<Object> objs = new ArrayList<Object>(1);
		objs.add(this.get(key));
		return objs;
	}	
	
}
