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

package org.teiid.query.eval;

import java.beans.BeanInfo;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.Reader;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;

/**
 * A simplistic script engine that supports root variable access and 0-ary methods on the subsequent objects.
 */
public final class TeiidScriptEngine extends AbstractScriptEngine implements Compilable {
	private static Reference<Map<Class<?>, Map<String, Method>>> properties;
	private static Pattern splitter = Pattern.compile("\\."); //$NON-NLS-1$
	
	@Override
	public Bindings createBindings() {
		return new SimpleBindings();
	}

	@Override
	public CompiledScript compile(String script) throws ScriptException {
		final String[] parts = splitter.split(script);
		final int[] indexes = new int[parts.length];
		for (int i = 1; i < parts.length; i++) {
			String string = parts[i];
			for (int j = 0; j < string.length(); j++) {
				if (!Character.isJavaIdentifierPart(string.charAt(j))) {
					throw new ScriptException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30431,string, string.charAt(j)));
				}
			}
			try {
				indexes[i] = Integer.parseInt(string);
			} catch (NumberFormatException e) {
				indexes[i] = -1;
			}
		}
		return new CompiledScript() {
			
			@Override
			public ScriptEngine getEngine() {
				return TeiidScriptEngine.this;
			}
			
			@Override
			public Object eval(ScriptContext sc) throws ScriptException {
				if (sc == null) {
					throw new NullPointerException();
				}
				Object obj = null;
				if (parts.length > 0) {
					obj = sc.getAttribute(parts[0]);
				}
				for (int i = 1; i < parts.length; i++) {
					if (obj == null) {
						return null;
					}
					String part = parts[i];
					Map<String, Method> methodMap = getMethodMap(obj.getClass());
					Method m = methodMap.get(part);
					if (m == null) {
						int index = indexes[i];
						if (index > 0) { //assume it's a list/array
							if (obj instanceof List) {
								try {
									obj = ((List<?>)obj).get(index - 1);
								} catch (IndexOutOfBoundsException e) {
									obj = null;
								}
								continue;
							}
							try {
								obj = FunctionMethods.array_get(obj, index);
								continue;
							} catch (FunctionExecutionException e) {
								throw new ScriptException(e);
							} catch (SQLException e) {
								throw new ScriptException(e);
							}
						}
						throw new ScriptException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31111, part, obj.getClass()));
					}
					try {
						obj = m.invoke(obj);
					} catch (IllegalAccessException e) {
						throw new ScriptException(e);
					} catch (InvocationTargetException e) {
						if (e.getCause() instanceof Exception) {
							throw new ScriptException((Exception) e.getCause());
						}
						throw new ScriptException(e);
					}
				}
				return obj;
			}
		};
	}
	
	public Map<String, Method> getMethodMap(Class<?> clazz) throws ScriptException {
		Map<Class<?>, Map<String, Method>> clazzMaps = null;
		Map<String, Method> methodMap = null; 
		if (properties != null) {
			clazzMaps = properties.get();
			if (clazzMaps != null) {
				methodMap = clazzMaps.get(clazz);
				if (methodMap != null) {
					return methodMap;
				}
			}
		}
		try {
			BeanInfo info = Introspector.getBeanInfo(clazz);
			PropertyDescriptor[] pds = info.getPropertyDescriptors();
			methodMap = new LinkedHashMap<String, Method>();
			if (pds != null) {
				for (int j = 0; j < pds.length; j++) {
					PropertyDescriptor pd = pds[j];
					if (pd.getReadMethod() == null || pd instanceof IndexedPropertyDescriptor) {
						continue;
					}
					String name = pd.getName();
					Method m = pd.getReadMethod();
					methodMap.put(name, m);
				}
			}
			MethodDescriptor[] mds = info.getMethodDescriptors();
			if (pds != null) {
				for (int j = 0; j < mds.length; j++) {
					MethodDescriptor md = mds[j];
					if (md.getMethod() == null || md.getMethod().getParameterTypes().length > 0 || md.getMethod().getReturnType() == Void.class || md.getMethod().getReturnType() == void.class) {
						continue;
					}
					String name = md.getName();
					Method m = md.getMethod();
					methodMap.put(name, m);
				}
			}
			if (clazzMaps == null) {
				clazzMaps = Collections.synchronizedMap(new LRUCache<Class<?>, Map<String,Method>>(100));
				properties = new SoftReference<Map<Class<?>,Map<String,Method>>>(clazzMaps);
			}
			clazzMaps.put(clazz, methodMap);
		} catch (IntrospectionException e) {
			throw new ScriptException(e);
		}
		return methodMap;
	}

	@Override
	public CompiledScript compile(Reader script) throws ScriptException {
		try {
			return compile(ObjectConverterUtil.convertToString(script));
		} catch (IOException e) {
			throw new ScriptException(e);
		}
	}
	
	@Override
	public ScriptEngineFactory getFactory() {
		throw new UnsupportedOperationException();
	}

	public Object eval(Reader reader, ScriptContext sc)
			throws ScriptException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object eval(String script, ScriptContext sc)
			throws ScriptException {
		throw new UnsupportedOperationException();
	}

}