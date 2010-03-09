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
package org.teiid.jboss.deployers;

import org.jboss.managed.api.ManagedObject;
import org.jboss.system.server.profileservice.persistence.PersistenceFactory;
import org.jboss.system.server.profileservice.persistence.component.AbstractComponentMapper;
import org.jboss.system.server.profileservice.persistence.xml.PersistedComponent;
import org.teiid.adminapi.impl.VDBMetaData;

/**
 * This class used in the Teiid deployer -jboss-beans.xml file.
 */
public class VDBMetadataComponentMapper extends AbstractComponentMapper {

	public VDBMetadataComponentMapper(PersistenceFactory persistenceFactory) {
		super(persistenceFactory);
	}

	@Override
	protected ManagedObject getComponent(Object attachment, PersistedComponent component, boolean create) {
		VDBMetaData metadata = (VDBMetaData)attachment;
		String vdbName = metadata.getName()+"_"+metadata.getVersion(); //$NON-NLS-1$
		if (vdbName.equals(component.getOriginalName())) {
			return getMOF().initManagedObject(metadata, VDBMetaData.class, vdbName, vdbName);
		}
		throw new IllegalStateException("could not find deployment " + component.getOriginalName());//$NON-NLS-1$
	}

	@Override
	protected void removeComponent(Object attachment,PersistedComponent component) {
		//TODO: What needs to be done here?
	}

	@Override
	protected void setComponentName(PersistedComponent component, ManagedObject mo) {
		VDBMetaData metadata = (VDBMetaData)mo.getAttachment();
		component.setName(metadata.getName()+"_"+metadata.getVersion()); //$NON-NLS-1$
	}

	@Override
	public String getType() {
		return VDBMetaData.class.getName();
	}

}
