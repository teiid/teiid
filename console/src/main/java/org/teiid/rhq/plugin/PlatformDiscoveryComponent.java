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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.teiid.rhq.admin.utils.SingletonConnectionManager;
import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.ConnectionConstants;


/**
 * This is the parent node for a MetaMatrix system
 */
public class PlatformDiscoveryComponent implements ResourceDiscoveryComponent {
	private static SingletonConnectionManager connMgr  = SingletonConnectionManager.getInstance();
	private static final Log LOG = LogFactory
			.getLog(PlatformDiscoveryComponent.class);
	   
	
	Map environmentMap = new HashMap();

    /**
	 * Review the javadoc for both {@link ResourceDiscoveryComponent} and
	 * {@link ResourceDiscoveryContext} to learn what you need to do in this
	 * method.
	 * 
	 * @see ResourceDiscoveryComponent#discoverResources(ResourceDiscoveryContext)
	 */
	public Set<DiscoveredResourceDetails> discoverResources(ResourceDiscoveryContext discoveryContext) throws InvalidPluginConfigurationException,
                                                                                                      Exception {
        try {
            
            String name = discoveryContext.getResourceType().getName();
            String desc = discoveryContext.getResourceType().getDescription();
            String version = ConnectionConstants.VERSION;
            
            LOG.info("Discovering " + desc); //$NON-NLS-1$
            
            
            // now perform your own discovery mechanism, if you have one. For each
            // resource discovered, you need to
            // create a details object that describe the resource that you
            // discovered.
            HashSet<DiscoveredResourceDetails> set = new HashSet<DiscoveredResourceDetails>();

            Set<String> systemkeys = null ;

            try {
            	systemkeys = connMgr.getInstallationSystemKeys();
            } catch (Exception e) {
            	systemkeys = new HashSet(1);
            	systemkeys.add("NotDefined");

            	// TODO
                // - when the serverList cannot be obtained
                
                // DO NOT throw exception, still want to create the
                // resource, but it will show not active / available
            }



            Iterator<String> serverIter = systemkeys.iterator();
            int hostCount = -1;
            while (serverIter.hasNext()) {
                hostCount++;
                String systemKey = serverIter.next();
                    
	                    DiscoveredResourceDetails resource = new DiscoveredResourceDetails(discoveryContext.getResourceType(),
	                                                                                       systemKey, name,
	                                                                                       version, desc, null, null);
	
	                    Configuration configuration = resource.getPluginConfiguration();
	                    configuration.put(new PropertySimple(Component.NAME, name));
	                    configuration.put(new PropertySimple(Component.IDENTIFIER, name));
	                    configuration.put(new PropertySimple(Component.SYSTEM_KEY, systemKey));
	                    
	
	                    set.add(resource); 

            }

            return set;
        } catch (InvalidPluginConfigurationException ipe) {
            throw ipe;
        } catch (Throwable t) {
            throw new InvalidPluginConfigurationException(t);
        }

    }
}