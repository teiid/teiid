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

package com.metamatrix.platform.config.spi.xml;

import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.core.util.ArgCheck;

public class HelperTestConfiguration {
	

    public static void validateModelContents(ConfigurationModelContainer config) throws ConfigurationException {
		validateConfigContents(config.getConfiguration());
		
		validateComponentTypes(config.getComponentTypes().values());
		validateResources(config.getResources());
    }

    public static void validateConfigContents(Configuration config) throws ConfigurationException {
    	if (config == null) {
    			throw new ConfigurationException("Unable to validate Configuration, the configuration is null"); //$NON-NLS-1$
    	}    		
    	
    	
    	if (config.getProperties() == null ||
    		config.getProperties().isEmpty()) {
    			throw new ConfigurationException("Configuration " + config.getID() + " does not contain any global properties"); //$NON-NLS-1$ //$NON-NLS-2$
    	}
        
      
        
        Collection vms = config.getVMComponentDefns();
        validateComponentDefns(vms);        
    	
    	Collection defns = config.getServiceComponentDefns();
    	validateComponentDefns(defns);
    	
    	Collection bindings = config.getConnectorBindings();
    	if (bindings != null && bindings.size() > 0) {
    		validateConnectorBindings(bindings);
    	}

    	Collection providers = config.getAuthenticationProviders();
    	if (providers != null && providers.size() > 0) {
    		validateAuthenticationProviders(providers);
    	}

    	Collection dcs = config.getDeployedComponents();
    	validateDeployedComponents(dcs);

    	
    	Collection hosts = config.getHosts();
    	validateHosts(hosts);
    		
    	
    	
    }
    
    public static void validateHosts(Collection defns) throws ConfigurationException {
    	
    	if (defns == null ||
    		defns.isEmpty()) {
    			throw new ConfigurationException("No hosts exist"); //$NON-NLS-1$
    			
    	}
    	
    	// this will throw a class cast exception if not the right type
    	for (Iterator it=defns.iterator(); it.hasNext(); ) {
    		Host t = (Host) it.next();  
    		validateHost(t);  				
    		
    	}
    	
    }
    
    public static void validateHost(Host h) throws ConfigurationException {
    
    	ArgCheck.isNotNull(h, "Host is null"); //$NON-NLS-1$
    	
		validateComponentObject(h);
		
    	ArgCheck.isNotNull(h.getProperties(), "Host does not have any properties"); //$NON-NLS-1$
    }
    
    public static void validateAuthenticationProviders(Collection providers) throws ConfigurationException {
    	
    	// this will throw a class cast exception if not the right type
    	for (Iterator it=providers.iterator(); it.hasNext(); ) {
    		AuthenticationProvider t = (AuthenticationProvider) it.next(); 
    		validateAuthenticationProvider(t);   				
    		
    	}
    	
    }
       
    public static void validateAuthenticationProvider(AuthenticationProvider h) throws ConfigurationException {
        
    	ArgCheck.isNotNull(h, "AuthenticationProvider is null"); //$NON-NLS-1$
    	
		validateComponentObject(h);
		
    	ArgCheck.isNotNull(h.getProperties(), "AuthenticationProvider does not have any properties"); //$NON-NLS-1$
   	
    	
    }    
    


    public static void validateComponentDefns(Collection defns) throws ConfigurationException {
    	
    	if (defns == null ||
    		defns.isEmpty()) {
    			throw new ConfigurationException("No component definitions exist"); //$NON-NLS-1$
    			
    	}
    	// this will throw a class cast exception if not the right type
    	for (Iterator it=defns.iterator(); it.hasNext(); ) {
    		ComponentDefn t = (ComponentDefn) it.next(); 
    		validateComponentDefn(t);   				
    		
    	}
    	
    }
    
    public static void validateComponentDefn(ComponentDefn defn) throws ConfigurationException {
    	ArgCheck.isNotNull(defn, "ComponentDefn is null"); //$NON-NLS-1$
    	
		validateComponentObject(defn);
		    
    } 
    
    public static void validateConnectorBindings(Collection bindings) throws ConfigurationException {
    	
    	// this will throw a class cast exception if not the right type
    	for (Iterator it=bindings.iterator(); it.hasNext(); ) {
    		ConnectorBinding t = (ConnectorBinding) it.next(); 
    		validateConnectorBinding(t);   				
    		
    	}
    	
    }
       
    public static void validateConnectorBinding(ConnectorBinding binding) throws ConfigurationException {
    
    	ArgCheck.isNotNull(binding, "Connector Binding is null"); //$NON-NLS-1$
    	
		validateComponentObject(binding);
		
    	ArgCheck.isNotNull(binding.getRoutingUUID(), "ConnectorBinding " + binding.getFullName() + " does not have a routing number"); //$NON-NLS-1$ //$NON-NLS-2$
				    
    }
    
    public static void validateDeployedComponents(Collection defns) throws ConfigurationException {
    	
    	if (defns == null ||
    		defns.isEmpty()) {
    			throw new ConfigurationException("No component definitions exist"); //$NON-NLS-1$
    			
    	}
    	// this will throw a class cast exception if not the right type
    	
    	for (Iterator it=defns.iterator(); it.hasNext(); ) {
    		DeployedComponent t = (DeployedComponent) it.next(); 
    		
    	   	ArgCheck.isNotNull(t, "ComponentObject is null"); //$NON-NLS-1$
        	ArgCheck.isNotNull(t.getID(), "ComponentObject ID is null"); //$NON-NLS-1$
        	String fullName = t.getFullName();
        	ArgCheck.isNotNull(t.getComponentTypeID(), "ComponentObject " + fullName + " does not have a valid component type id"); //$NON-NLS-1$ //$NON-NLS-2$
        	ArgCheck.isNotNull(t.getDeployedComponentDefnID(), "ComponentObject " + fullName + " does not have a deployed component defn id"); //$NON-NLS-1$ //$NON-NLS-2$
           	ArgCheck.isNotNull(t.getHostID(), "ComponentObject " + fullName + " does not have a host id"); //$NON-NLS-1$ //$NON-NLS-2$
           	ArgCheck.isNotNull(t.getVMComponentDefnID(), "ComponentObject " + fullName + " does not have a vm id"); //$NON-NLS-1$ //$NON-NLS-2$
          	ArgCheck.isNotNull(t.getServiceComponentDefnID(), "ComponentObject " + fullName + " does not have the service component defin id"); //$NON-NLS-1$ //$NON-NLS-2$

        	//       	ArgCheck.isNotNull(c.getCreatedBy(), "ComponentObject " + fullName + " does not have a created by name"); //$NON-NLS-1$ //$NON-NLS-2$
 //       	ArgCheck.isNotNull(c.getLastChangedBy(), "ComponentObject " + fullName + " does not have a last changed by name"); //$NON-NLS-1$ //$NON-NLS-2$
 //       	ArgCheck.isNotNull(c.getCreatedDate(), "ComponentObject " + fullName + " does not have a created by date"); //$NON-NLS-1$ //$NON-NLS-2$
 //       	ArgCheck.isNotNull(c.getLastChangedDate(), "ComponentObject " + fullName + " does not have a last changed by date"); //$NON-NLS-1$ //$NON-NLS-2$

 //   		validateComponentObject(t);   				
    		
    	}
    	
    }
    
    public static void validateComponentTypes(Collection types) throws ConfigurationException {
    	
    	if (types == null ||
    		types.isEmpty()) {
    			throw new ConfigurationException("No component types exist"); //$NON-NLS-1$
    			
    	}
    	// this will throw a class cast exception if not the right type
    	
    	for (Iterator it=types.iterator(); it.hasNext(); ) {
    		ComponentType t = (ComponentType) it.next();
			validateComponentType(t);   				
    		
    	}
    	
    }
    
    public static void validateResources(Collection resources) throws ConfigurationException {
    	
    	if (resources == null ||
    		resources.isEmpty()) {
    			throw new ConfigurationException("No resources exist"); //$NON-NLS-1$
    			
    	}
    	// this will throw a class cast exception if not the right type
    	for (Iterator it=resources.iterator(); it.hasNext(); ) {
    		SharedResource t = (SharedResource) it.next();    	
   			validateComponentObject(t); 
    	}
    	
    	
    }
    
    public static void validateComponentObject(ComponentObject c) throws ConfigurationException {
    
    	ArgCheck.isNotNull(c, "ComponentObject is null"); //$NON-NLS-1$
    	ArgCheck.isNotNull(c.getID(), "ComponentObject ID is null"); //$NON-NLS-1$
    	String fullName = c.getFullName();
    	ArgCheck.isNotNull(c.getComponentTypeID(), "ComponentObject " + fullName + " does not have a valid component type id"); //$NON-NLS-1$ //$NON-NLS-2$
    	ArgCheck.isNotNull(c.getCreatedBy(), "ComponentObject " + fullName + " does not have a created by name"); //$NON-NLS-1$ //$NON-NLS-2$
    	ArgCheck.isNotNull(c.getLastChangedBy(), "ComponentObject " + fullName + " does not have a last changed by name"); //$NON-NLS-1$ //$NON-NLS-2$
    	ArgCheck.isNotNull(c.getCreatedDate(), "ComponentObject " + fullName + " does not have a created by date"); //$NON-NLS-1$ //$NON-NLS-2$
    	ArgCheck.isNotNull(c.getLastChangedDate(), "ComponentObject " + fullName + " does not have a last changed by date"); //$NON-NLS-1$ //$NON-NLS-2$
    	
    }
    
    public static void validateComponentType(ComponentType c) throws ConfigurationException {
    
    	ArgCheck.isNotNull(c, "ComponentType is null"); //$NON-NLS-1$
    	ArgCheck.isNotNull(c.getID(), "ComponentType ID is null"); //$NON-NLS-1$
    	String fullName = c.getFullName();
    	ArgCheck.isNotNull(c.getCreatedBy(), "ComponentType " + fullName + " does not have a created by name"); //$NON-NLS-1$ //$NON-NLS-2$
    	ArgCheck.isNotNull(c.getLastChangedBy(), "ComponentType " + fullName + " does not have a last changed by name"); //$NON-NLS-1$ //$NON-NLS-2$
    	ArgCheck.isNotNull(c.getCreatedDate(), "ComponentType " + fullName + " does not have a created by date"); //$NON-NLS-1$ //$NON-NLS-2$
    	ArgCheck.isNotNull(c.getLastChangedDate(), "ComponentType " + fullName + " does not have a last changed by date"); //$NON-NLS-1$ //$NON-NLS-2$

	//NOT ALL TYPES HAVE DEFINITIONS
//    	ArgCheck.isNotNull(c.getComponentTypeDefinitions(), fullName + " does not have any component definitions");
//    	ArgCheck.isNotEmpty(c.getComponentTypeDefinitions(), fullName + " does not have any component definitions");

    	
    }
                

}
