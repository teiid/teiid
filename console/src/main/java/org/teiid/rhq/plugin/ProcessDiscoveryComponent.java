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
package org.teiid.rhq.plugin;

import java.util.Collection;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.VMComponent;



/**
 * 
 * The discovery component class for the MetaMatrix server node
 * 
 */
public class ProcessDiscoveryComponent extends NodeChildrenDiscoveryComponent {
 
    
	 Collection<Component> getComponents(Connection conn, Facet parent) throws ConnectionException {
		 return conn.discoverComponents(ConnectionConstants.ComponentType.Runtime.Process.TYPE, parent.getComponentIdentifier());
	 }
	 
		protected void addAdditionalProperties(Configuration configuration, Component component) throws InvalidPluginConfigurationException {
            configuration.put(new PropertySimple(VMComponent.PORT, ((VMComponent)component).getPort()));   
   
		}
}