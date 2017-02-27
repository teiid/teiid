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
package org.teiid.translator.object;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.script.ScriptException;

import org.teiid.core.util.StringUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.util.ObjectUtil;


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

	private ObjectScriptEngine readEngine=null;
	private ObjectScriptEngine writeEngine=null;
	
	private Map<String, Class<?>> registeredClasses = new HashMap<String, Class<?>>(); // fullClassName, Class
	private Map<String, Class<?>> tableNameClassMap = new HashMap<String, Class<?>>(); // simpleClassName(i.e., tableName), Class 

	private ObjectDataTypeManager dataTypeManager = null;
	
	public ClassRegistry() {
		dataTypeManager = new ObjectDataTypeManager();
		init();
	}
	
	public ClassRegistry(ObjectDataTypeManager odtm) {
		this.dataTypeManager = odtm;
		init();
	}
	
	private void init()
	{
		readEngine = new ObjectScriptEngine(true);
		writeEngine = new ObjectScriptEngine(false);
	}
	
	public synchronized void registerClass(Class<?> clz) {
		 
		registeredClasses.put(clz.getName(), clz);	
		tableNameClassMap.put(clz.getSimpleName(), clz);
		 	
	}
	
	public synchronized void unregisterClass(Class<?> clz)  {
		registeredClasses.remove(clz.getName());	
		tableNameClassMap.remove(clz.getSimpleName());	
	}

	public ObjectDataTypeManager getObjectDataTypeManager() {
		return this.dataTypeManager;
	}
	
	public ObjectScriptEngine getReadScriptEngine() {
		return readEngine;
	}
	
	public Map<String, Method> getReadClassMethods(String className) throws TranslatorException {	
		Map<String, Method> methodMap = getClassMethods(readEngine, className);
		
		if (methodMap != null) return methodMap;

		String msg = ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21001, "read", className);
		throw new TranslatorException(msg);

	}	
	
	public Map<String, Method> getWriteClassMethods(String className) throws TranslatorException {	
		
		Map<String, Method> methodMap = getClassMethods(writeEngine, className);
		
		if (methodMap != null) return methodMap;

		String msg = ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21001, "write", className);
		throw new TranslatorException(msg);

	}	
	
	private synchronized Map<String, Method> getClassMethods(ObjectScriptEngine engine, String className) throws TranslatorException {	
		
		Class<?> clz = registeredClasses.get(className);
		if (clz == null) {
			clz = tableNameClassMap.get(className);
		}
		
		if (clz != null) {	
			try {
				return engine.getMethodMap(clz); 

			} catch (ScriptException e) {
				throw new TranslatorException(e);
			}
		}

		return null;
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

		final Class<?> argType = m.getParameterTypes()[0];
		Object[] params = new Object[] {arg};
		if (arg != null && !argType.isAssignableFrom(arg.getClass())) {
			params = new Object[] {StringUtil.valueOf(arg.toString(), argType)};
		}

    	try {
    		return m.invoke(api, params);
    	} catch (java.lang.IllegalArgumentException ia) {
    		
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21016, new Object[] {(arg != null ? arg : "Null"), m.getName(), api.getClass().getName(), argType.getName() }));
    		
    	}
    } 
	
    /**
     * Call to execute the getter method 
     * @param m is the method to execute
     * @param api is the object to execute the method on 
     * @return Object return value
     * @throws Exception
     */
	public static Object executeGetMethod(Method m, Object api) throws Exception {
		Object[] params = new Object[] {};
    	try {
    		return m.invoke(api, params);
    	} catch (java.lang.IllegalArgumentException ia) { 		
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21016, new Object[] {("N/A"), m.getName(), api.getClass().getName(), "N/A" }));    		
    	}
    } 
    
	public static Method findMethod(Map<String, Method> mapMethods, String methodName, String className) throws TranslatorException {
		
		return ObjectUtil.findMethod(mapMethods, methodName);

	}
    
    public void cleanUp() {
		readEngine = null;
		writeEngine = null;
		
    	registeredClasses.clear();
    	tableNameClassMap.clear();

    }

    /**
     * Utility method for debugging to print out the key and methods for a class
     * @param m
     */
    public static void print(Map m) {
    	Iterator<Object> i = m.keySet().iterator();
    	while (i.hasNext()) {
    		Object k = (Object) i.next();
    		Method v = (Method) m.get(k);
    		System.out.println("---- " + k.toString() + ":" + v.getName());
    	}


    }
}
