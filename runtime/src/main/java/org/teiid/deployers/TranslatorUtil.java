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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorProperty;

public class TranslatorUtil {
	
    public static final String DEPLOYMENT_NAME = "deployment-name"; //$NON-NLS-1$
	
	public static Map<Method, TranslatorProperty> getTranslatorProperties(Class<?> attachmentClass) {
		Map<Method, TranslatorProperty> props = new TreeMap<Method,  TranslatorProperty>(new Comparator<Method>() {
			@Override
			public int compare(Method o1, Method o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		buildTranslatorProperties(attachmentClass, props);
		return props;
	}

	private static void buildTranslatorProperties(Class<?> attachmentClass, Map<Method, TranslatorProperty> props){
		Class<?>[] baseInterfaces = attachmentClass.getInterfaces();
		for (Class<?> clazz:baseInterfaces) {
			buildTranslatorProperties(clazz, props);
		}
		Class<?> superClass = attachmentClass.getSuperclass();
		if (superClass != null) {
			buildTranslatorProperties(superClass, props);
		}
		Method[] methods = attachmentClass.getMethods();
		for (Method m:methods) {
			TranslatorProperty tp = m.getAnnotation(TranslatorProperty.class);
			if (tp != null) {
				props.put(m, tp);
			}
		}
	}	
	
	public static ExecutionFactory buildExecutionFactory(VDBTranslatorMetaData data) throws TeiidException {
		ExecutionFactory executionFactory;
		try {
			Class<?> executionClass = data.getExecutionFactoryClass();
			Object o = executionClass.newInstance();
			if(!(o instanceof ExecutionFactory)) {
				 throw new TeiidException(RuntimePlugin.Event.TEIID40024, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40024, executionClass));
			}
			executionFactory = (ExecutionFactory)o;
			injectProperties(executionFactory, data);
			executionFactory.start();
			return executionFactory;
		} catch (InvocationTargetException e) {
			throw new TeiidException(RuntimePlugin.Event.TEIID40025, e);
		} catch (IllegalAccessException e) {
			throw new TeiidException(RuntimePlugin.Event.TEIID40026, e);
		} catch (InstantiationException e) {
			throw new TeiidException(CorePlugin.Event.TEIID10036, e);
		}
	}
	
	private static void injectProperties(ExecutionFactory ef, final VDBTranslatorMetaData data) throws InvocationTargetException, IllegalAccessException, TeiidException{
		Map<Method, TranslatorProperty> props = TranslatorUtil.getTranslatorProperties(ef.getClass());
		Map p = data.getPropertiesMap();
		TreeMap<String, String> caseInsensitiveProps = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
		VDBTranslatorMetaData parent = data.getParent();
		while (parent != null) {
			for (Map.Entry<String, String> entry : parent.getPropertiesMap().entrySet()) {
				if (!caseInsensitiveProps.containsKey(entry.getKey()) && entry.getValue() != null) {
					caseInsensitiveProps.put(entry.getKey(), entry.getValue());
				}
			}
			parent = parent.getParent();
		}
		synchronized (p) {
			caseInsensitiveProps.putAll(p);
		}
		caseInsensitiveProps.remove(DEPLOYMENT_NAME);
		for (Method method:props.keySet()) {
			TranslatorProperty tp = props.get(method);
			String propertyName = getPropertyName(method);
			String value = caseInsensitiveProps.remove(propertyName);
			
			if (value != null) {
				Method setterMethod = getSetter(ef.getClass(), method);
				setterMethod.invoke(ef, convert(value, method.getReturnType()));
			} else if (tp.required()) {
				 throw new TeiidException(RuntimePlugin.Event.TEIID40027, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40027, tp.display()));
			}
		}
		caseInsensitiveProps.remove(Translator.EXECUTION_FACTORY_CLASS);
		if (!caseInsensitiveProps.isEmpty()) {
			LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40001, caseInsensitiveProps.keySet(), data.getName()));
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
	
	public static Method getSetter(Class<?> clazz, Method method) throws SecurityException, TeiidException {
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
				 throw new TeiidException(RuntimePlugin.Event.TEIID40028, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40028, setter, method.getName()));
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
	
	public static String getTranslatorName(ExecutionFactory factory) {
		org.teiid.translator.Translator translator = factory.getClass().getAnnotation(org.teiid.translator.Translator.class);
		if (translator == null) {
			return null;
		}
		return translator.name();
	}

	public static VDBTranslatorMetaData buildTranslatorMetadata(ExecutionFactory factory, String moduleName) {
		
		org.teiid.translator.Translator translator = factory.getClass().getAnnotation(org.teiid.translator.Translator.class);
		if (translator == null) {
			return null;
		}
		
		VDBTranslatorMetaData metadata = new VDBTranslatorMetaData();
		metadata.setName(translator.name());
		metadata.setDescription(translator.description());
		metadata.setExecutionFactoryClass(factory.getClass());
		metadata.setModuleName(moduleName);
		
		try {
			Object instance = factory.getClass().newInstance();
			Map<Method, TranslatorProperty> tps = TranslatorUtil.getTranslatorProperties(factory.getClass());
			for (Method m:tps.keySet()) {
				Object defaultValue = getDefaultValue(instance, m, tps.get(m));
				if (defaultValue != null) {
					metadata.addProperty(getPropertyName(m), defaultValue.toString());
				}
			}
		} catch (InstantiationException e) {
			// ignore
		} catch (IllegalAccessException e) {
			// ignore
		}
		
		return metadata;
	}
	
	private static Object convert(Object instance, Method method, TranslatorProperty prop) {
		Class<?> type = method.getReturnType();
		String[] allowedValues = null;
		Method getter = null;
		boolean readOnly = false;
		if (type == Void.TYPE) { //check for setter
			Class<?>[] types = method.getParameterTypes();
			if (types.length != 1) {
				 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40029, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40029, method));
			}
			type = types[0];
			try {
				getter = instance.getClass().getMethod("get" + method.getName(), (Class[])null); //$NON-NLS-1$
			} catch (Exception e) {
				try {
					getter = instance.getClass().getMethod("get" + method.getName().substring(3), (Class[])null); //$NON-NLS-1$
				} catch (Exception e1) {
					//can't find getter, won't set the default value
				}
			}
		} else if (method.getParameterTypes().length != 0) {
			 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40029, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40029, method));
		} else {
			getter = method;
			try {
				TranslatorUtil.getSetter(instance.getClass(), method);
			} catch (Exception e) {
				readOnly = true;
			}
		}
		Object defaultValue = null;
		if (prop.required()) {
			if (prop.advanced()) {
				 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40031, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40031,method));
			}
		} else if (getter != null) {
			try {
				defaultValue = getter.invoke(instance, (Object[])null);
			} catch (Exception e) {
				//no simple default value
			}
		}
		if (type.isEnum()) {
			Object[] constants = type.getEnumConstants();
			allowedValues = new String[constants.length];
			for( int i=0; i<constants.length; i++ ) {
                allowedValues[i] = ((Enum<?>)constants[i]).name();
            }
			type = String.class;
			if (defaultValue != null) {
				defaultValue = ((Enum<?>)defaultValue).name();
			}
		}
		if (!(defaultValue instanceof Serializable)) {
			defaultValue = null; //TODO
		}
		return defaultValue;
	}
	
	public static Object getDefaultValue(Object instance, Method method, TranslatorProperty prop) {
		return convert(instance, method, prop);
	}	
}
