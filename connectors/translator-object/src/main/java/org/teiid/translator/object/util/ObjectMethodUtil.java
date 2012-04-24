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

package org.teiid.translator.object.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import org.teiid.core.util.ReflectionHelper;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.TranslatorException;

/**
 * ObjectMethodUtil provides utility methods
 */
public final class ObjectMethodUtil  {

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
	public static Object executeMethod(Method m, Object api, List<Object> parms)
			throws TranslatorException {
		try {
			if (parms != null) {
				return m.invoke(api, parms.toArray());
			}
			return m.invoke(api, (Object[]) null);
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
	

	
	public static Object createObject(String objectClassName,
			Collection<?> ctors, ClassLoader loader) throws TranslatorException {
		try {

			return ReflectionHelper.create(objectClassName, ctors, loader);
		} catch (Exception e1) {
			throw new TranslatorException(e1);
		}
	}

}
