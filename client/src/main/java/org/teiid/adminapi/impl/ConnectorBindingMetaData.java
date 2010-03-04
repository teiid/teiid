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
package org.teiid.adminapi.impl;

import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementObjectID;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.ConnectorBinding;

@ManagementObject(componentType=@ManagementComponent(type="teiid",subtype="connector"))
public class ConnectorBindingMetaData extends AdminObjectImpl implements ConnectorBinding {

	private static final long serialVersionUID = -4865836616882247016L;
	private transient Object type;

	@ManagementProperty(description="Connector Binding Name", readOnly=true)
	@ManagementObjectID(type="binding")
	public String getName() {
		return super.getName();
	}    
	
	@Override
	@ManagementProperty(description="RAR file name", readOnly=true)	
	public String getRARFileName() {
		return getPropertyValue("rar-name");
	}

	@Override
	@ManagementProperty(description="JNDI name", readOnly=true)	
	 public String getJNDIName() {
		return getPropertyValue("jndi-name");
	 }
	
	public void setComponentType(Object type) {
		this.type = type;
	}
	
	public Object getComponentType() {
		return this.type;
	}
	
	public String toString() {
		return getName();
	}
}
