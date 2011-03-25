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
package org.teiid.deployers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.jboss.deployers.spi.DeploymentException;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.core.TeiidException;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorProperty;

public class TranslatorUtil {
	
	public static Map<Method, TranslatorProperty> getTranslatorProperties(Class<?> attachmentClass) {
		Map<Method, TranslatorProperty> props = new HashMap<Method,  TranslatorProperty>();
		buildTranslatorProperties(attachmentClass, props);
		return props;
	}

	public static Properties getTranslatorPropertiesAsProperties(Class<?> attachmentClass) {
		Properties props = new Properties();
		try {
			Object instance = attachmentClass.newInstance();
			Map<Method, TranslatorProperty> tps = TranslatorUtil.getTranslatorProperties(attachmentClass);
			for (Method m:tps.keySet()) {
				Object defaultValue = ManagedPropertyUtil.getDefaultValue(instance, m, tps.get(m));
				if (defaultValue != null) {
					props.setProperty(getPropertyName(m), defaultValue.toString());
				}
			}
		} catch (InstantiationException e) {
			// ignore
		} catch (IllegalAccessException e) {
			// ignore
		}
		return props;
	}
	
	private static void buildTranslatorProperties(Class<?> attachmentClass, Map<Method, TranslatorProperty> props){
		Method[] methods = attachmentClass.getMethods();
		for (Method m:methods) {
			TranslatorProperty tp = m.getAnnotation(TranslatorProperty.class);
			if (tp != null) {
				props.put(m, tp);
			}
		}
		// Now look at the base interfaces
		Class[] baseInterfaces = attachmentClass.getInterfaces();
		for (Class clazz:baseInterfaces) {
			buildTranslatorProperties(clazz, props);
		}
		Class superClass = attachmentClass.getSuperclass();
		if (superClass != null) {
			buildTranslatorProperties(superClass, props);
		}
	}	
	
	public static ExecutionFactory buildExecutionFactory(Translator data) throws DeploymentException {
		ExecutionFactory executionFactory;
		try {
			String executionClass = data.getPropertyValue(TranslatorMetaData.EXECUTION_FACTORY_CLASS);
			Object o = ReflectionHelper.create(executionClass, null, Thread.currentThread().getContextClassLoader());
			if(!(o instanceof ExecutionFactory)) {
				throw new DeploymentException(RuntimePlugin.Util.getString("invalid_class", executionClass));//$NON-NLS-1$	
			}
			
			executionFactory = (ExecutionFactory)o;
			injectProperties(executionFactory, data);
			executionFactory.start();
			return executionFactory;
		} catch (TeiidException e) {
			throw new DeploymentException(e);
		} catch (InvocationTargetException e) {
			throw new DeploymentException(e);
		} catch (IllegalAccessException e) {
			throw new DeploymentException(e);
		}
	}
	
	private static void injectProperties(ExecutionFactory ef, final Translator data) throws InvocationTargetException, IllegalAccessException, DeploymentException{
		Map<Method, TranslatorProperty> props = TranslatorUtil.getTranslatorProperties(ef.getClass());
		Map p = data.getProperties();
		TreeMap<String, String> caseInsensitivProps = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
		caseInsensitivProps.putAll(p);
		for (Method method:props.keySet()) {
			TranslatorProperty tp = props.get(method);
			String propertyName = getPropertyName(method);
			String value = caseInsensitivProps.remove(propertyName);
			
			if (value != null) {
				Method setterMethod = getSetter(ef.getClass(), method);
				setterMethod.invoke(ef, convert(value, method.getReturnType()));
			} else if (tp.required()) {
				throw new DeploymentException(RuntimePlugin.Util.getString("required_property_not_exists", tp.display())); //$NON-NLS-1$
			}
		}
		caseInsensitivProps.remove(Translator.EXECUTION_FACTORY_CLASS);
		if (!caseInsensitivProps.isEmpty()) {
			LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("undefined_translator_props", caseInsensitivProps.keySet(), data.getName())); //$NON-NLS-1$
		}
	}
	
	public static String getPropertyName(Method method) {
		String result = method.getName();
		if (result.startsWith("get") || result.startsWith("set")) { //$NON-NLS-1$ //$NON-NLS-2$
			return result.substring(3);
		}
		else if (result.startsWith("is")) { //$NON-NLS-1$
			return result.substring(2);
		}
		return result;
	}
	
	public static Method getSetter(Class<?> clazz, Method method) throws SecurityException, DeploymentException {
		String setter = method.getName();
		if (method.getName().startsWith("get")) { //$NON-NLS-1$
			setter = "set"+setter.substring(3);//$NON-NLS-1$
		}
		else if (method.getName().startsWith("is")) { //$NON-NLS-1$
			setter = "set"+setter.substring(2); //$NON-NLS-1$
		}
		else {
			setter = "set"+method.getName().substring(0,1).toUpperCase()+method.getName().substring(1); //$NON-NLS-1$
		}
		try {
			return clazz.getMethod(setter, method.getReturnType());
		} catch (NoSuchMethodException e) {
			try {
				return clazz.getMethod(method.getName(), method.getReturnType());
			} catch (NoSuchMethodException e1) {
				throw new DeploymentException(RuntimePlugin.Util.getString("no_set_method", setter, method.getName())); //$NON-NLS-1$
			}
		}
	}
	
	private static Object convert(Object value, Class<?> type) {
		if(value.getClass() == type) {
			return value;
		}
		
		if (value instanceof String) {
			String str = (String)value;
			return StringUtil.valueOf(str, type);
		}
		return value;
	}
}
