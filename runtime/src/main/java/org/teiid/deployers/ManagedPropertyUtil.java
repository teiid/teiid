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

import java.util.HashSet;

import org.jboss.managed.api.Fields;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.ManagedPropertyImpl;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.teiid.resource.cci.TranslatorProperty;

public class ManagedPropertyUtil {
	private static final String TEIID_PROPERTY = "teiid-property"; //$NON-NLS-1$
	
	public static ManagedProperty convert(TranslatorProperty prop) {
		return createProperty(prop.name(), SimpleMetaType.resolve(prop.type().getName()), 
				prop.display(), prop.description(), prop.required(), prop.readOnly(), prop.defaultValue(),
				prop.advanced(), prop.masked(), prop.allowed());
	}
	
	public static ManagedProperty convert(ExtendedPropertyMetadata prop) {
		return createProperty(prop.name(), SimpleMetaType.resolve(prop.type()), 
				prop.display(), prop.description(), prop.required(), prop.readOnly(), prop.defaultValue(),
				prop.advanced(), prop.masked(), prop.allowed());
	}
	
	public static ManagedProperty createProperty(String name,
			SimpleMetaType type, String displayName, String description,
			boolean mandatory, boolean readOnly, String defaultValue) {

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
			boolean mandatory, boolean readOnly, String defaultValue, boolean advanced,
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
