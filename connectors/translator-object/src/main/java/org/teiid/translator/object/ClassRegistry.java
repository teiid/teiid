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
import java.util.List;
import java.util.Map;

import javax.script.ScriptException;

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
	
	private Map<String, Class<?>> registeredClasses = new HashMap<String, Class<?>>();
	private Map<String, Class<?>> classTableMap = new HashMap<String, Class<?>>();
	
	public synchronized void registerClass(Class<?> clz) throws TranslatorException {
		try {
			// preload methods
			readEngine.getMethodMap(clz);
			
			registeredClasses.put(clz.getName(), clz);	
			classTableMap.put(clz.getSimpleName(), clz);	
		} catch (ScriptException e) {
			throw new TranslatorException(e);
		}	
	}
	
	public TeiidScriptEngine getReadScriptEngine() {
		return readEngine;
	}
	
	public Map<String, Method> getReadClassMethods(String className) throws TranslatorException {	
		try {
			return readEngine.getMethodMap(registeredClasses.get(className));
		} catch (ScriptException e) {
			throw new TranslatorException(e);
		}

	}	

	public Class<?> getRegisteredClass(String className) {
		return registeredClasses.get(className);
	}
	
	public List<Class<?>> getRegisteredClasses() {
		return new ArrayList<Class<?>>(registeredClasses.values());
	}
	
	public Class<?> getRegisteredClassToTable(String tableName) {
		return classTableMap.get(tableName);
	}	

}
