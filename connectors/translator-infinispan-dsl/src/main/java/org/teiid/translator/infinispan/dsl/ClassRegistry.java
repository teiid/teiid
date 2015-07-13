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
package org.teiid.translator.infinispan.dsl;

import java.beans.BeanInfo;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.script.ScriptException;

import org.teiid.core.util.LRUCache;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.translator.TranslatorException;


/**
 * The ClassRegistryUtil is used to manage the classes registered with the cache, as well as,
 * a map of the associated {@link Method methods}, so that the methods don't have to be re-loaded
 * as different parts of the logic require their use.
 *  
 * @author vanhalbert
 *
 */
public class ClassRegistry {
	
	public static final String OBJECT_NAME = "o"; //$NON-NLS-1$

	private TeiidScriptEngine readEngine = new TeiidScriptEngine();
	
	private Map<String, Class<?>> registeredClasses = new HashMap<String, Class<?>>(3); // fullClassName, Class
	private Map<String, Class<?>> tableNameClassMap = new HashMap<String, Class<?>>(3); // simpleClassName(i.e., tableName), Class 
	private Map<String, Map<String, Method>> classReadMethodMap = new HashMap<String, Map<String, Method>>(100);
	private Map<String, Map<String, Method>> classWriteMethodMap = Collections.synchronizedMap(new LRUCache<String, Map<String,Method>>(100));

	public synchronized void registerClass(Class<?> clz) throws TranslatorException {
			// preload methods
	
			registeredClasses.put(clz.getName(), clz);	
			tableNameClassMap.put(clz.getSimpleName(), clz);
			
			try {
				classReadMethodMap.put(clz.getName(),  readEngine.getMethodMap(registeredClasses.get(clz.getName())));
			} catch (ScriptException e) {
				throw new TranslatorException(e);
			}

			classWriteMethodMap.put(clz.getSimpleName(),getWriteMethodMap(clz));

	}
	
	public synchronized void unregisterClass(Class<?> clz) throws TranslatorException {
			registeredClasses.remove(clz.getName());	
			tableNameClassMap.remove(clz.getSimpleName());	
			classReadMethodMap.remove(clz.getName());
	}

	
	public TeiidScriptEngine getReadScriptEngine() {
		return readEngine;
	}
	
	public synchronized Map<String, Method> getReadClassMethods(String className) throws TranslatorException {	
		Map<String, Method> methodMap = null; 
		methodMap = classReadMethodMap.get(className);
		if (methodMap != null) {
			return methodMap;
		}		
		
		throw new TranslatorException("Class Registration Error:  no class read methods have been generated for class " + className);

	}	
	
	public synchronized Map<String, Method> getWriteClassMethods(String tableName) throws TranslatorException {	

		Map<String, Method> methodMap = null; 
		methodMap = classWriteMethodMap.get(tableName);
		if (methodMap != null) {
			return methodMap;
		}
		
		throw new TranslatorException("Class Registration Error:  no class write methods have been generated for class/table " + tableName);

	}		

	public Class<?> getRegisteredClass(String className) {
		return registeredClasses.get(className);
	}
	
	public List<Class<?>> getRegisteredClasses() {
		return new ArrayList<Class<?>>(registeredClasses.values());
	}
	
	public Class<?> getRegisteredClassUsingTableName(String tableName) {
		return tableNameClassMap.get(tableName);
	}	
	
    /**
     * Call to execute the set method 
     * @param m is the method to execute
     * @param api is the object to execute the method on 
     * @param arg is the argument to be passed to the set method
     * @return Object return value
     * @throws Exception
     */
	public static Object executeSetMethod(Method m, Object api, Object arg) throws Exception {
        
    	List<Object> args = new ArrayList<Object>(1);
    	args.add(arg);
        return m.invoke(api, args.toArray());
    } 
    
    private static Map<String, Method> getWriteMethodMap(Class<?> clazz) throws TranslatorException {
    	LinkedHashMap<String, Method> methodMap = new LinkedHashMap<String, Method>();
    	
		try {
			BeanInfo info = Introspector.getBeanInfo(clazz);
			PropertyDescriptor[] pds = info.getPropertyDescriptors();
			
			if (pds != null) {
				for (int j = 0; j < pds.length; j++) {
					PropertyDescriptor pd = pds[j];
					if (pd.getWriteMethod() == null || pd instanceof IndexedPropertyDescriptor) {
						continue;
					}
					String name = pd.getName();
					Method m = pd.getWriteMethod();
					methodMap.put(name, m);
				}
			}
			MethodDescriptor[] mds = info.getMethodDescriptors();
			if (pds != null) {
				for (int j = 0; j < mds.length; j++) {
					MethodDescriptor md = mds[j];
					if (md.getMethod() == null || md.getMethod().getParameterTypes().length !=1 || (md.getMethod().getReturnType() != Void.class && md.getMethod().getReturnType() != void.class)) {
						continue;
					}
					String name = md.getName();
					Method m = md.getMethod();
					methodMap.put(name, m);
				}
			}

		} catch (IntrospectionException e) {
			throw new TranslatorException(e);
		}
		return methodMap;
	}
    
    public void cleanUp() {
    	registeredClasses.clear();
    	tableNameClassMap.clear();
    	classReadMethodMap.clear();
    	classWriteMethodMap.clear();

    }


}
