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

package com.metamatrix.common.config.model;

import java.io.Serializable;

import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.namedobject.BaseID;

/**
 * DeployedComponent represents a <code>Component</code> that is declared deployed
 * to a specified machine.  The <code>componentID<code> represents the component that
 * has been deployed.   It can either be a VM component or a Service Component.
 * If its a VM component, the <code>vmComponentID<code> will be null, because a
 * deployed VM cannot be deployed within another VM.  The DeployedComponent does
 * not utilize a ComponentType, therefore, it is set to null;
 * </p>
 * As you will notice, there is no reference held to a <code>Configuration</code>.
 * This is done so that a <code>DeployedComponent</code> can be serialized
 * independently from having to also serialize the whole configuration of objects.
 */
public class BasicDeployedComponent extends BasicComponentObject implements DeployedComponent, Serializable {

	private static final String ENABLED="component.enabled";
	
    private ConfigurationID configurationID;

    /**
     * componentID is the component that is deployed
     * @link aggregationByValue
     * @supplierCardinality 1
     * @label id
     */
    private ServiceComponentDefnID componentID;

    /**
     * @supplierCardinality 1
     * @label hosted on
     */
    private HostID hostID;

    /**
     * @supplierCardinality *
     * @clientCardinality 1
     */
    private VMComponentDefnID vmComponentID;

    /**
     * Constructor takes a <code>ComponentID, HostID, </code> and <code> Collection </code> of system components to declare a component as being deployed.
     *  @param deployedID is the DeployedComponentID
     *  @param configID is the ConfigurationID
     *  @param hostID is the HostID
     *  @param vmComponentID is the VMComponentDefnID
     *  @param serviceComponentID is the ServiceComponentID (null if this is a deployed VM)
     */
    public BasicDeployedComponent(DeployedComponentID deployedId, ConfigurationID configId, HostID hostId, VMComponentDefnID vmId, ComponentTypeID deployedTypeID) {
        this(deployedId, configId, hostId, vmId, null,  deployedTypeID);

    }

    public BasicDeployedComponent(DeployedComponentID deployedId, ConfigurationID configId, HostID hostId, VMComponentDefnID vmId, ServiceComponentDefnID serviceId, ComponentTypeID deployedTypeID) {
        super(deployedId, deployedTypeID);
        this.configurationID = configId;
        this.hostID = hostId;
        this.vmComponentID = vmId;
        if (serviceId != null) {
            this.componentID = serviceId;
        }
        
    }

    public BasicDeployedComponent(BasicDeployedComponent deployedComponent ) {
        super(deployedComponent.getID(), deployedComponent.getComponentTypeID());
        this.configurationID = deployedComponent.getConfigurationID();
        this.hostID = deployedComponent.getHostID();
        this.vmComponentID = deployedComponent.getVMComponentDefnID();
        if (deployedComponent.getServiceComponentDefnID() != null) {
            this.componentID = deployedComponent.getServiceComponentDefnID();
        }
        this.addProperties(deployedComponent.getEditableProperties());
    }
    
    

    public boolean isEnabled() {
    	String enabled = this.getProperty(ENABLED);
    	if (enabled == null || enabled.trim().length() == 0) {
    		return true;
    	}
    	return (new Boolean(enabled).booleanValue());

    }
    
    public void setIsEnabled(boolean enable) {
    		this.addProperty(ENABLED, new Boolean(enable).toString());
    }
    
    /**
     * Indicates whether this object represents a deployed
     * service component definition (returns true) or
     * a deployed vm component definition (returns false).
     * @return true if this is a deployed service, false if it is
     * a deployed vm
     */
    public boolean isDeployedService(){
        if (componentID == null) {
            return false;
        }
        if (componentID instanceof ConnectorBindingID) {
            return false;
        }
        return true;
    }
    
    /**
     * Indicates whether this object represents a deployed
     * connector.  If {@see #isDeployedService } returns false,
     * this will always return false.
     * @return true if this is a deployed connector.
     */
    public boolean isDeployedConnector() {
    	if (!isDeployedService()) {
    		if (componentID instanceof ConnectorBindingID) {
    			return true;
    		}    		
    	} 
    	
    	return false;	
    	
    }
    
    
    public ServiceComponentDefnID getServiceComponentDefnID() {
	    return componentID;
    }

    public ConfigurationID getConfigurationID() {
	    return configurationID;
    }

    public VMComponentDefnID getVMComponentDefnID() {
        return vmComponentID;
    }

    public HostID getHostID() {
	    return hostID;
    }

    public ComponentDefnID getDeployedComponentDefnID() {
        if (componentID != null) {
            return componentID;
        }

        return vmComponentID;
    }

    public ComponentDefn getDeployedComponentDefn(Configuration configuration) {
	     return configuration.getComponentDefn(getDeployedComponentDefnID());
    }

    public boolean isDependentUpon(BaseID componentObjectId) {
        if (componentObjectId instanceof ConfigurationID) {
            return configurationID.equals(componentObjectId);
        } else if (componentObjectId instanceof HostID)  {
            return hostID.equals(componentObjectId);
        } else if (componentObjectId instanceof VMComponentDefnID) {
            return vmComponentID.equals(componentObjectId);
        } else if (componentObjectId instanceof ServiceComponentDefnID) {
            if (componentID == null) {
                return false;
            }

            return componentID.equals(componentObjectId);
        } 

        return false;
    }

    void setConfigurationID(ConfigurationID configID){
	    configurationID = configID;
    }
    
   public synchronized Object clone() {
    	 return new BasicDeployedComponent(this);
    }
    

    /**
     * Returns a string representing the name of the object.  This has been
     * overriden for GUI display purposes - the Console only wants to display
     * the "name" (not the "fullname") of a component object.Me
     * @return the string representation of this instance.
     */
    public String toString(){
	    return BasicServiceComponentDefn.getDisplayableName(this.getName());
    }
}

