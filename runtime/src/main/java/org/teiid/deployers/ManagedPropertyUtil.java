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
import java.lang.reflect.Method;
import java.util.HashSet;

import org.jboss.managed.api.Fields;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.ManagedPropertyImpl;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.translator.TranslatorProperty;

public class ManagedPropertyUtil {
	private static final String TEIID_PROPERTY = "teiid-property"; //$NON-NLS-1$
	
	public static ManagedProperty convert(Object instance, Method method, TranslatorProperty prop) {
		return (ManagedProperty)convert(instance, method, prop, false);
	}

	private static Object convert(Object instance, Method method, TranslatorProperty prop, boolean needDefaultValue) {
		Class<?> type = method.getReturnType();
		String[] allowedValues = null;
		Method getter = null;
		boolean readOnly = false;
		if (type == Void.TYPE) { //check for setter
			Class<?>[] types = method.getParameterTypes();
			if (types.length != 1) {
				throw new TeiidRuntimeException("TranslatorProperty annotation should be placed on valid getter or setter method, " + method + " is not valid."); //$NON-NLS-1$ //$NON-NLS-2$
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
			throw new TeiidRuntimeException("TranslatorProperty annotation should be placed on valid getter or setter method, " + method + " is not valid."); //$NON-NLS-1$ //$NON-NLS-2$
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
				throw new TeiidRuntimeException("TranslatorProperty annotation should not both be advanced and required " + method); //$NON-NLS-1$
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
		if (needDefaultValue) {
			return defaultValue;
		}
		return createProperty(TranslatorUtil.getPropertyName(method), SimpleMetaType.resolve(type.getName()), 
				prop.display(), prop.description(), prop.required(), readOnly, (Serializable)defaultValue,
				prop.advanced(), prop.masked(), allowedValues);
	}
	
	public static Object getDefaultValue(Object instance, Method method, TranslatorProperty prop) {
		return convert(instance, method, prop, true);
	}	
	
	public static ManagedProperty convert(ExtendedPropertyMetadata prop) {
		return createProperty(prop.name(), SimpleMetaType.resolve(prop.type()), 
				prop.display(), prop.description(), prop.required(), prop.readOnly(), prop.defaultValue(),
				prop.advanced(), prop.masked(), prop.allowed());
	}
	
	public static ManagedProperty createProperty(String name,
			MetaType type, String displayName, String description,
			boolean mandatory, boolean readOnly, Serializable defaultValue) {

		DefaultFieldsImpl fields = new DefaultFieldsImpl(name);
		fields.setDescription(description);
		fields.setField(Fields.MAPPED_NAME,displayName);
		fields.setMetaType(type);
		fields.setField(Fields.MANDATORY, SimpleValueSupport.wrap(mandatory));
		fields.setField(Fields.READ_ONLY, SimpleValueSupport.wrap(readOnly));
		fields.setField(TEIID_PROPERTY, SimpleValueSupport.wrap(true));
		
		if (defaultValue != null) {
			fields.setField(Fields.DEFAULT_VALUE, SimpleValueSupport.wrap(defaultValue));
		}
		return  new ManagedPropertyImpl(fields);		
	}
	
	public static ManagedProperty createProperty(String name,
			SimpleMetaType type, String displayName, String description,
			boolean mandatory, boolean readOnly, Serializable defaultValue, boolean advanced,
			boolean masked, String[] allowed) {
		
		ManagedProperty mp = createProperty(name, type, displayName, description, mandatory, readOnly, defaultValue);
		mp.setField("advanced",  SimpleValueSupport.wrap(advanced));//$NON-NLS-1$	
		mp.setField("masked",  SimpleValueSupport.wrap(masked));//$NON-NLS-1$
		if (allowed != null) {
			HashSet<MetaValue> values = new HashSet<MetaValue>();
			for (String value:allowed) {
				values.add(SimpleValueSupport.wrap(value));
			}
			mp.setField(Fields.LEGAL_VALUES, values);
		}		
		return mp;		
	}
	
	public static void markAsTeiidProperty(ManagedProperty mp) {
		mp.setField(TEIID_PROPERTY, SimpleValueSupport.wrap(true)); 
	}
}
