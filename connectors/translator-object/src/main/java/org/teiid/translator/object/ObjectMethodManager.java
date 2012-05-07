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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.core.util.StringUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.util.ObjectMethodUtil;

/**
 * ObjectMethodManager caches the methods for the classes that
 * are being utilized, and provides utility methods for finding a method
 * and executing.
 */
public class ObjectMethodManager extends ConcurrentHashMap<String, Object> { // className, ClassMethods
	public static final String GET = "get";
	public static final String SET = "set";
	public static final String IS = "is";
	
	public final class ClassMethods {
		
		private Class<?> clz = null;
		private Map<String, Method> getters = new HashMap<String, Method>(10);
		private Map<String, Method> setters = new HashMap<String, Method>(10);
		private Map<String, Method> is = new HashMap<String, Method>(5);
		
		ClassMethods(final Class<?> clzz) {
			this.clz = clzz;
		}
		
		public Class getClassIdentifier() {
			return this.clz;
		}
		public String getClassName() {
			return this.clz.getName();
		}
		
		public boolean hasMethods() {
			return (!getters.isEmpty() || !is.isEmpty() || !setters.isEmpty());
		}
		
		public Map<String, Method> getGetters() {
			return getters;
		}
		
		void addGetter(String methodName, Method g) {
			this.getters.put(methodName, g);
		}
		
		Method getGetMethod(String name) {
			return this.getters.get(name);
		}
		
		public Map<String, Method> getSetters() {
			return setters;
		}
		
		void addSetter(String methodName, Method s) {
			this.setters.put(methodName, s);
		}
		
		Method getSetMethod(String name) {
			return this.setters.get(name);
		}	
		
		public Map<String, Method> getIses() {
			return this.is;
		}
		
		void addIsMethod(String methodName, Method i) {
			this.is.put(methodName, i);
		}
		
		Method getIsMethod(String name) {
			return this.is.get(name);
		}			
	}
	
	@SuppressWarnings("unchecked")
	public static ObjectMethodManager initialize(boolean firstLetterUpperCaseInColumName, ClassLoader classloader) throws TranslatorException {
		return initialize(Collections.EMPTY_LIST, firstLetterUpperCaseInColumName, classloader);
	}
	
	@SuppressWarnings("unchecked")
	public static ObjectMethodManager initialize(String commaSeparatedNames, boolean firstLetterUpperCaseInColumName, ClassLoader classloader) throws TranslatorException {
		List<String> classNames = null;
		if (commaSeparatedNames != null) {
			classNames = StringUtil.getTokens(commaSeparatedNames, ",");
		} else {
			classNames = Collections.EMPTY_LIST;
		}
		return initialize(classNames, firstLetterUpperCaseInColumName, classloader);
	}
	
	public static ObjectMethodManager initialize(List<String> classNames, boolean firstLetterUpperCaseInColumName, ClassLoader classloader) throws TranslatorException {
		if (classNames == null) classNames = Collections.EMPTY_LIST;
		
		ObjectMethodManager osmm = new ObjectMethodManager(firstLetterUpperCaseInColumName);
		osmm.loadClassNames(classNames, classloader);
		
		return osmm;
	}
	
	public void loadClassNames(List<String> classNames, ClassLoader classloader) throws TranslatorException {
		for (String clzName: classNames) {
			loadClassByName(clzName, classloader);
		}
	}
	
	public Class<?> loadClassByName(String className, ClassLoader classLoader) throws TranslatorException {
		ClassMethods clzzMethods = getClassMethods(className);
		if (clzzMethods == null) {
			Class<?> clz = loadClass(className, classLoader);
			clzzMethods = loadClassMethods(clz);
			return clz;
		}
		return clzzMethods.getClass();
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 8576602118928356693L;
	
	private ObjectMethodManager(boolean firstLetterUpperCaseInColumName) {
		this.firstLetterUpperCaseInColumName = firstLetterUpperCaseInColumName;
	}
	
	public Object getIsValue(String methodName, Object object)
			throws TranslatorException {

		final Method m = findIsMethod(object.getClass(), methodName);
		if (m == null) {
			throw new TranslatorException(
					ObjectPlugin.Util
							.getString(
									"ObjectMethodManager.noMethodFound", new Object[] { methodName, object.getClass().getName() })); //$NON-NLS-1$    
		}
		return ObjectMethodUtil.executeMethod(m, object, (Object[]) null);
	}

	public  Object getGetValue(String methodName, Object object)
			throws TranslatorException {

		final Method m = findGetterMethod(object.getClass(), methodName);
		if (m == null) {
		throw new TranslatorException(
				ObjectPlugin.Util
						.getString(
								"ObjectMethodManager.noMethodFound", new Object[] {methodName, object.getClass().getName()})); //$NON-NLS-1$    
		}
		return ObjectMethodUtil.executeMethod(m, object, (Object[]) null);
	}
	
	public Object setValue(String methodName, Object object)
			throws TranslatorException {

		final Method m = findSetterMethod(object.getClass(), methodName);
		if (m == null) {
			throw new TranslatorException(
					ObjectPlugin.Util
							.getString(
									"ObjectMethodManager.noMethodFound", new Object[] { methodName, object.getClass().getName() })); //$NON-NLS-1$    
		}
		List<Object> parms = new ArrayList<Object>(1);
		parms.add(object);
		return ObjectMethodUtil.executeMethod(m, object, parms);
	}

	public Method findGetterMethod(Class<?> api, String methodName) throws TranslatorException {
		return getClassMethods(api).getGetMethod(methodName);
	}
	
	public Method findSetterMethod(Class<?> api, String methodName) throws TranslatorException {
		return getClassMethods(api).getSetMethod(methodName);
	}
	
	public Method findIsMethod(Class<?> api, String methodName) throws TranslatorException {
		return getClassMethods(api).getIsMethod(methodName);
	}	
	
	
	/**
	 * Call to format the method name based on how the columnName (NameInSource) is being modeled.
	 * If its not UpperCase, then the first character will be UpperCased.  This is done so that the
	 * method name follows the JavaBeans naming convention (i.e., getMethod or setMethod)
	 * @param prefix it the value is the fist part of the method name (i.e, get,set,is)
	 * @param columnName is the attribute to calling get,set or is on the object
	 * @param columNameStartsWithUpperCase is a boolean, indicating if the first character is UpperCased.
	 * @return
	 * @see ObjectExecutionFactory#firstLetterUpperCaseInColumName()
	 */
	public  String formatMethodName(String prefix, String columnName) {
		if (this.firstLetterUpperCaseInColumName) return prefix + columnName;
		
		return prefix + columnName.substring(0, 1).toUpperCase() + columnName.substring(1);
	}
	
	
	public boolean isFirstLetterUpperCaseInColumName() {
		return this.firstLetterUpperCaseInColumName;
	}
	
	public static synchronized final Class<?> loadClass(final String className,
			final ClassLoader classLoader) throws TranslatorException {
		try {
			Class<?> cls = null;
			if (classLoader == null) {
				cls = Class.forName(className.trim());
			} else {
				cls = Class.forName(className.trim(), true, classLoader);
			}
			return cls;
		
		} catch (ClassNotFoundException e) {
			throw new TranslatorException(
					ObjectPlugin.Util
							.getString(
									"ObjectMethodManager.objectClassNotFound", new Object[] {className})); //$NON-NLS-1$           
		}

	}
	
	public ClassMethods getClassMethods(String className) {
		return (ClassMethods) this.get(className);
	}
	
	private ClassMethods getClassMethods(Class<?> api) throws TranslatorException {
		ClassMethods clzMethods = getClassMethods(api.getName());
		if (clzMethods == null) {
			loadClassMethods(api);
			clzMethods = this.getClassMethods(api);
		}
		return clzMethods;

	}
	
	
	private synchronized ClassMethods loadClassMethods(Class<?> clzz) {
		Method[] methods = clzz.getDeclaredMethods();
		ClassMethods clzMethods = new ClassMethods(clzz);
		
		for (int i=0; i<methods.length; i++) {
			Method m = methods[i];
			if (m.getName().startsWith(GET)) {
				clzMethods.addGetter(m.getName(), m);
				// commenting out the caching of the setter methods
//			} else if (m.getName().startsWith(SET)) {
//				clzMethods.addSetter(m.getName(), m);
			} else if (m.getName().startsWith(IS)) {
				clzMethods.addIsMethod(m.getName(), m);
			}
		}
		this.put(clzz.getName(), clzMethods);
		return clzMethods;
	}
	
	private boolean firstLetterUpperCaseInColumName = true;

}
