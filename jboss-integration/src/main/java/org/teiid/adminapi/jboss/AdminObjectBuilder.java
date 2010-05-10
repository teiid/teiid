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
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValue;
import org.teiid.jboss.IntegrationPlugin;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;


public class AdminObjectBuilder {

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
							MapCompositeValueSupport map = (MapCompositeValueSupport)value;
						}
					}
					else if (type.isCollection()) {
						List list = new ArrayList();
						List<ManagedObject> managedObjects = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(value);
						for (ManagedObject mo:managedObjects) {
							list.add(buildAO(mo, mo.getAttachment().getClass()));							
						}
						PropertiesUtils.setBeanProperty(t, mp.getName(), list);
					}
				}
			}
			return clazz.cast(t);
		} catch (InstantiationException e) {
			throw new MetaMatrixRuntimeException(e, IntegrationPlugin.Util.getString("class_not_found", clazz.getName())); //$NON-NLS-1$
		} catch (IllegalAccessException e) {
			throw new MetaMatrixRuntimeException(e, IntegrationPlugin.Util.getString("class_not_found", clazz.getName())); //$NON-NLS-1$
		}
	}
}
