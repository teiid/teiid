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
package org.jboss.as.quickstarts.datagrid.hotrod.query.domain;


import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.InfinispanConnection;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;


/**
 * Sample cache of objects
 * 
 * @author vhalbert
 *
 */
@SuppressWarnings({ "nls" })
public class PersonCacheSource extends HashMap <Object, Object> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -727553658250070494L;
	
	public static final String PERSON_CACHE_NAME = "PersonsCache";
	
	public static final String PERSON_CLASS_NAME = Person.class.getName();
	public static final String PHONENUMBER_CLASS_NAME = PhoneNumber.class.getName();
	public static final String PHONETYPE_CLASS_NAME = PhoneType.class.getName();
	
	public static Map<String, Class<?>> mapOfCaches = new HashMap<String, Class<?>>(1);
	
	public static Descriptor DESCRIPTOR;
	
	public static final int NUMPERSONS = 10;
	public static final int NUMPHONES = 2;
	
	private static Map <Object, Object> OBJECTS;
	
	static {
		mapOfCaches.put(PersonCacheSource.PERSON_CACHE_NAME, Person.class);
		try {
			DESCRIPTOR = createDescriptor();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		OBJECTS = loadCache();
	}
	
	
	public static InfinispanConnection createConnection() {
		return new InfinispanConnection() {

			@Override
			public Class<?> getType(String cacheName)  {
				return Person.class;
			}


			@Override
			public Map<String, Class<?>> getCacheNameClassTypeMapping() {
				return mapOfCaches;
			}
			
			@Override
			public String getPkField(String cacheName) {
				return "id";
			}

			@Override
			public Descriptor getDescriptor(String cacheName)
					throws TranslatorException {
				return DESCRIPTOR;
			}

			@Override
			public Map<?, ?> getCache(String cacheName) {
				return OBJECTS;
			}


			@Override
			public QueryFactory getQueryFactory(String cacheName) {
				return null;
			}

			public List<Class> getRegisteredClasses() {
				ArrayList<Class> al = new ArrayList<Class>(3);
				al.add(Person.class);
				al.add(PhoneNumber.class);
				al.add(PhoneType.class);
				return al;
				
			}
			


		};
	}
	
	public static void loadCache(Map<Object, Object> cache) {
		PhoneType[] types = new PhoneType[] {PhoneType.HOME, PhoneType.MOBILE, PhoneType.WORK};
		int t = 0;
		
		for (int i = 1; i <= NUMPERSONS; i++) {
			
			List<PhoneNumber> pns = new ArrayList<PhoneNumber>();
			double d = 0;
			for (int j = 1; j <= NUMPHONES; j++) {
				
				PhoneNumber pn = new PhoneNumber("(111)222-345" + j, types[t++]);
				
				if (t > 2) t = 0;
				
				
				pns.add(pn);
			}
			
			Person p = new Person();
			p.setId(i);
			p.setName("Person " + i);
			p.setPhones(pns);
				
			cache.put(String.valueOf(i), p);

		}
	}

	public static Map<Object, Object>  loadCache() {
		PersonCacheSource tcs = new PersonCacheSource();
		PersonCacheSource.loadCache(tcs);
		return tcs;		
	}
	
	public List<Object> getAll() {
		return new ArrayList<Object>(this.values());
	}
	
	public List<Object> get(int key) {
		List<Object> objs = new ArrayList<Object>(1);
		objs.add(super.get(key));
		return objs;
	}	
	
    private static Descriptor createDescriptor() throws Exception {
	
	InputStream is = new FileInputStream("./src/test/resources/addressbook.protobin");
	FileDescriptorSet fds = FileDescriptorSet.parseFrom(is);
    Map<String, FileDescriptor> fdl = new HashMap<String, FileDescriptor>(); 
	
	  try { 
          FileDescriptor current = null; 
          for (FileDescriptorProto fdp : fds.getFileList()) { 
              final List<String> dependencyList = fdp.getDependencyList(); 
              final FileDescriptor[] fda = new FileDescriptor[dependencyList 
                      .size()]; 

              for (int i = 0; i < fda.length; i++) { 
                  FileDescriptor fddd = fdl.get(dependencyList.get(i)); 
                  if (fddd == null) { 
                      // missing imports! - this should not happen  unless you left off the --include_imports directive 
                  } else { 
                      fda[i] = fddd; 
                  } 
              } 
              current = FileDescriptor.buildFrom(fdp, fda); 
              fdl.put(current.getName(), current); 
          } 

          // the "fdl" object now has all the descriptors - grab the  one you need. 

      } catch (DescriptorValidationException e) { 
          // panic ? 
      } 
	
	is.close();
	

	FileDescriptor fdes = fdl.get("addressbook.proto");
	List<Descriptor> all = fdes.getMessageTypes();
	return all.get(0);
}

	
}
