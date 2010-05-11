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
package org.teiid.adminapi.jboss;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jboss.managed.api.ManagedCommon;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.managed.plugins.factory.AbstractManagedObjectFactory;
import org.jboss.managed.plugins.factory.ManagedObjectFactoryBuilder;
import org.jboss.metatype.api.types.CollectionMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValue;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.templates.TranslatorMetadataICF;



public class AdminObjectBuilder {

	private ManagedObjectFactory mof = ManagedObjectFactoryBuilder.create();
	
	public AdminObjectBuilder() {
		this.mof.setInstanceClassFactory(TranslatorMetaData.class, new TranslatorMetadataICF(this.mof));	
	}
	
	public static <T> T buildAO(ManagedCommon mc, Class<T> clazz) {
		
		try {
			Object t = clazz.newInstance();
			
			Map<String, ManagedProperty> managedProperties = mc.getProperties(); 
			for (ManagedProperty mp: managedProperties.values()) {
				MetaValue value = mp.getValue();
				if (value != null) {
					MetaType type = value.getMetaType();
					if (type.isSimple()) {
						PropertiesUtils.setBeanProperty(t, mp.getMappedName(), ((SimpleValue)value).getValue());
					}
					else if (type.isProperties()) {
						
					}
					else if (type.isComposite()) {
						if (value instanceof MapCompositeValueSupport) {
							Object myValue = MetaValueFactory.getInstance().unwrap(value);
							PropertiesUtils.setBeanProperty(t, mp.getMappedName(), myValue);
						}
					}
					else if (type.isCollection()) {
						List list = new ArrayList();

						MetaType elementType = ((CollectionMetaType) type).getElementType();
						if (elementType == AbstractManagedObjectFactory.MANAGED_OBJECT_META_TYPE) {
							List<ManagedObject> managedObjects = (List<ManagedObject>) MetaValueFactory.getInstance().unwrap(value);
							for (ManagedObject mo : managedObjects) {
								list.add(buildAO(mo, mo.getAttachment().getClass()));
							}
						}
						else if (elementType == SimpleMetaType.STRING) {
							list.addAll((List<String>) MetaValueFactory.getInstance().unwrap(value));
						}
						
						PropertiesUtils.setBeanProperty(t, mp.getMappedName(), list);
					}
				}
			}
			return clazz.cast(t);
		} catch (InstantiationException e) {
			throw new TeiidRuntimeException(e, IntegrationPlugin.Util.getString("class_not_found", clazz.getName())); //$NON-NLS-1$
		} catch (IllegalAccessException e) {
			throw new TeiidRuntimeException(e, IntegrationPlugin.Util.getString("class_not_found", clazz.getName())); //$NON-NLS-1$
		}
	}
	
	public <T> T buildAdminObject(ManagedCommon mc, Class<T> clazz) {
		try {
			Object t = clazz.newInstance();			
	        ManagedObject mo = mof.initManagedObject(t, "teiid", "translator"); //$NON-NLS-1$ //$NON-NLS-2$		
			for (ManagedProperty mp : mc.getProperties().values()) {
				ManagedProperty dsProp = mo.getProperty(mp.getName());
				if (dsProp != null) {
					if (mp.getValue() != null) {
						dsProp.setValue(mp.getValue());
					}
				}
			}  
			return clazz.cast(t);
		} catch (InstantiationException e) {
			throw new TeiidRuntimeException(e, IntegrationPlugin.Util.getString("class_not_found", clazz.getName())); //$NON-NLS-1$
		} catch (IllegalAccessException e) {
			throw new TeiidRuntimeException(e, IntegrationPlugin.Util.getString("class_not_found", clazz.getName())); //$NON-NLS-1$
		}
	}
}
