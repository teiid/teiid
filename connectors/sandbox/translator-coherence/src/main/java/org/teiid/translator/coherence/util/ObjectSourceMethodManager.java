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

package org.teiid.translator.coherence.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.core.util.ReflectionHelper;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.coherence.CoherencePlugin;

/**
 * ObjectSourceMethodManager caches the methods for the classes that
 * are being utilized, and provides utility methods for finding a method
 * and executing.
 */
public class ObjectSourceMethodManager {

	private static ObjectSourceMethodManager osmm = new ObjectSourceMethodManager();

	private Map<Class<?>, Method[]> classToMethodMap = new ConcurrentHashMap<Class<?>, Method[]>();

	public ObjectSourceMethodManager() {

	}

	/**
	 * Call to execute the method
	 * 
	 * @param m
	 *            is the method to execute
	 * @param api
	 *            is the object to execute the method on
	 * @param parms
	 *            are the parameters to pass when the method is executed
	 * @return Object return value
	 * @throws Exception
	 */
	public static Object executeMethod(Method m, Object api, Object[] parms)
			throws TranslatorException {
		try {
			return m.invoke(api, parms);
		} catch (InvocationTargetException x) {
			x.printStackTrace();
			Throwable cause = x.getCause();
			System.err.format("invocation of %s failed: %s%n",
					"set" + m.getName(), cause.getMessage());
			LogManager.logError(LogConstants.CTX_CONNECTOR, "Error calling "
					+ m.getName() + ":" + cause.getMessage());
			throw new TranslatorException(x.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new TranslatorException(e.getMessage());
		}
	}

	/**
	 * Call to execute the method
	 * 
	 * @param m
	 *            is the method to execute
	 * @param api
	 *            is the object to execute the method on
	 * @param parms
	 *            are the parameters to pass when the method is executed
	 * @return Object return value
	 * @throws Exception
	 */
	public static Object executeMethod(Method m, Object api, List parms)
			throws TranslatorException {
		try {
			return m.invoke(api, parms.toArray());
		} catch (InvocationTargetException x) {
			x.printStackTrace();
			Throwable cause = x.getCause();
			System.err.format("invocation of %s failed: %s%n",
					"set" + m.getName(), cause.getMessage());
			LogManager.logError(LogConstants.CTX_CONNECTOR, "Error calling "
					+ m.getName() + ":" + cause.getMessage());
			throw new TranslatorException(x.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new TranslatorException(e.getMessage());
		}
	}

	public static Object createObject(String objectClassName)
			throws TranslatorException {
		try {

			return ReflectionHelper.create(objectClassName, null, null);
		} catch (Exception e1) {
			throw new TranslatorException(e1);
		}

	}

	public static Object getValue(String methodName, Object object)
			throws TranslatorException {
		try {
			final Method m = osmm
					.getMethod(object.getClass(), methodName, null);

			return m.invoke(object, (Object[]) null);
		} catch (TranslatorException t) {
			throw t;
		} catch (InvocationTargetException x) {
			Throwable cause = x.getCause();
			System.err.format("invocation of %s failed: %s%n", "get"
					+ methodName, cause.getMessage());
			LogManager.logError(LogConstants.CTX_CONNECTOR, "Error calling get"
					+ methodName + ":" + cause.getMessage());
			throw new TranslatorException(x.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new TranslatorException(e.getMessage());
		}

	}

	public static Method getMethod(Class api, String methodName, List argumentsClasses)
			throws TranslatorException {
		
		try {
			Method[] namedMethods = findMethods(methodName, getMethodsFromAPI(api));
			if (namedMethods != null && namedMethods.length == 1) {
				return namedMethods[0];
			}
	
			Method m = findBestMethod(api, methodName, argumentsClasses);
	
			if (m == null) {
				throw new NoSuchMethodException(
						CoherencePlugin.Util
								.getString(
										"ObjectSourceMethodManager.No_method_implemented_for", new Object[] {methodName, api.getName()})); //$NON-NLS-1$           
			}
			return m;
		
		} catch (NoSuchMethodException nsme) {
			String msg = CoherencePlugin.Util
			.getString(
					"ObjectSourceMethodManager.No_method_implemented_for", new Object[] {methodName, api.getName()});
			System.out.println(msg);
			throw new TranslatorException(msg); //$NON-NLS-1$           
		} catch (Exception e) {
			e.printStackTrace();
	        final String msg = CoherencePlugin.Util.getString("ObjectSourceMethodManager.No_method_implemented_for", new Object[] {methodName, api.getName()}); //$NON-NLS-1$
			throw new TranslatorException(msg);	
		}		
	}

	private static Method[] findMethods(String methodName, Method[] methods) {
		if (methods == null || methods.length == 0) {
			return null;
		}
		final ArrayList result = new ArrayList();
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equals(methodName)) {
				result.add(methods[i]);
			}
		}
		return (Method[]) result.toArray(new Method[result.size()]);
	}

	private static synchronized Method[] getMethodsFromAPI(Class api)
			throws SecurityException {
		Method[] ms = osmm.classToMethodMap.get(api);
		if (ms != null) {
			return ms;
		}
		Method[] methods = api.getMethods();
		osmm.classToMethodMap.put(api, methods);
		return methods;
	}
	/**
	 * Call to find the best method on a class, by passing the 
	 * cache
	 * @param objectClass
	 * @param methodName
	 * @param argumentsClasses
	 * @return
	 * @throws TranslatorException
	 */
	private static Method findBestMethod(Class objectClass, String methodName,
			List argumentsClasses) throws SecurityException, NoSuchMethodException {
		
		ReflectionHelper rh = new ReflectionHelper(objectClass);

		if (argumentsClasses == null) {
			argumentsClasses = Collections.EMPTY_LIST;
		}
		return rh.findBestMethodWithSignature(methodName, argumentsClasses);

	}

}
