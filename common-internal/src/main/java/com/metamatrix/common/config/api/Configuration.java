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

package com.metamatrix.common.config.api;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.namedobject.BaseID;

/**
 * Configuration represents deployable components (e.g., service and vm components),
 * components definitions, global properties, and additional information.  Once the configuration is
 * flagged as "Released", it is then available to be deployed.  Once it is flagged as "Deployed",
 * it cannot be changed.  The configuration must be versioned, and then changes can be made to the
 * new versioned copy. 
 */
public interface Configuration extends ComponentObject {
    
    /*
    * These static finals are used to determine which Configuration a 
    * configuration object represents.
    */
    static final String NEXT_STARTUP = "Next Startup"; //$NON-NLS-1$
    
    /**
    * These configuration ID's can be used to determine which Configuration a
    * ConfigurationID represents using equals.
    */
    static final ConfigurationID NEXT_STARTUP_ID = new ConfigurationID(NEXT_STARTUP);
    
    static final String COMPONENT_TYPE_NAME = "Configuration"; //$NON-NLS-1$
    static final ComponentTypeID CONFIG_COMPONENT_TYPE_ID = new ComponentTypeID(COMPONENT_TYPE_NAME);


    String getName();

    /**
     * Returns the <code>ConfigurationInfo</code> that provides basic information
     * about this configuration.
     * @return ConfigurationInfo
     */
    ConfigurationInfo getInfo();

    /**
     * Returns a <code>UnmodifiableProperties</code> object containing the current configuration properties.
     */
    Properties getProperties();

    /**
     * Returns the property value for the specified property name.
     * @return String value
     */
    String getProperty(String name);

    /**
     *  Returns a <code>Map</code> of the component definitions that have been created for this configuration.
     *  @return Map of component definitions
     */
    Map getComponentDefns();

    /**
     * This method returns a ComponentDefn implementation, given
     * a ComponentDefnID.  The returned object can be cast to
     * the subclass which corresponds to the subclass of the
     * componentID parameter.  For example, if a
     * ServiceComponentDefnID is passed in, then the returned
     * object will be a ServiceComponentDefn.
     * @param componentID ID of the ComponentDefn to retrieve from
     * this configuration object
     * @return ComponentDefn impl corresponding to the ID
     */
    ComponentDefn getComponentDefn(ComponentDefnID componentID);

    /**
     * Returns a Collection of ComponentDefnID objects that are of the
     * indicated type.  Note that the type must be an exact match, no
     * component defns of a sub-type of the indicated type will be
     * returned.
     * @param componentTypeID ComponentTypeID of desired ComponentDefns
     * contained in this Configuration
     * @return Collection of ComponentDefnID objects - use
     * {@link #getComponentDefn} method to retrieve the full
     * ComponentDefn object
     */
    Collection getComponentDefnIDs(ComponentTypeID componentTypeID);


    /**
     *  Returns only the dependent properites for the specified component object.
     *  This excludes the properties that are defined at components level.  To get the properties defined
     *  at this components level, call #getProperties() on the component.
     *
     *  The following is the top-down hierarchy as to the order properties are
     *  assembled for inheritance:
     *
     *      A.  VM ComponentDefn
     *          -  Configuration
     *          -  VM definition
     *
     *      B.  Service ComponentDefn
     *          -  Configuration
     *          -  Service definition
     *
     *      C.  VM Deployed Component
     *          -  Configuration
     *          -  VM Definition
     *          -  VM Deployed Component
     *
     *      D.  Service Deployed Component
     *          -  VM Deployed Component (and all dependencies)
     *          -  Service Definition
     *          -  Service Deployed Component
     *
     * @param componentObjectID is the component object for which all properties it inherits will be returned
     * @return Properties that are inherited
     *
     * @see #getAllPropertiesForComponent
     */
    Properties getDependentPropsForComponent(BaseID componentObjectID) ;

    /**
     *  Returns the all properites for the specified component object.  This includes the
     *  dependent propertie, as well as, the properties defined at its level.
     *  @param componentObjectID is the id for the component object which all properties will be returned.
     *  @return Properties that are inherited
     *
     * @see getDependentPropsForComponent
     */
    Properties getAllPropertiesForComponent(BaseID componentObjectID) ;

    /**
     *  Returns a <code>Collection</code> of deployed components, keyed by the component id.
     *  @return Collection of deployed components
     */
    Collection getDeployedComponents();

    /**
     *  Returns a <code>Collection</code> of deployed components that have been deployed for
     *  that specified component definition.
     *  @param componentDefnID is the definition that has been deployed
     *  @return Collection of deployed components
     */
    Collection getDeployedComponents(ComponentDefnID componentDefnID);
    
    /**
     *  Returns a <code>Collection</code> of deployed components that have been deployed for
     *  that specified component definition and belong to a specific ProductServiceConfiguration.
     *  There can be multiple deployed components because the ProductServiceConfig could be
     *  deployed to multiple VMs and/or Hosts.
     *  @param componentDefnID is the definition that has been deployed
     *  @param pscID is the ProductServiceConfig for which the component belongs
     *  @return Collection of deployed components that belong to the ProductServiceConfig
     */
//    Collection getDeployedComponents(ComponentDefnID componentDefnID, ProductServiceConfigID pscID);
    
    
    /**
     * Returns true if the PSC is deployed to any host.
     * @returns boolean true if the PSC is deployed
     */
 //   boolean isPSCDeployed(ProductServiceConfigID pscID);

    
    /**
     *  Returns the deployed component for the specified id
     *  @param deployedComponentID for the deployed component
     *  @return DeployedComponent
     */
    DeployedComponent getDeployedComponent(DeployedComponentID deployedComponentID);
    

    /**
     *  Returns a deployed VM component that was deployed on the specified host.
     *  @param vmID identifies the VM definition that was deployed
     *  @param hostID is the host the VM was deployed to.
     *  @return DeployedComponent
     */
    VMComponentDefn getVMForHost(VMComponentDefnID vmID, HostID hostID);

    /**
     *  Returns a deployed VM component that was deployed on the specified host.
     *  @param vmID identifies the VM definition that was deployed
     *  @param hostID is the host the VM was deployed to.
     *  @return DeployedComponent
     */
    VMComponentDefn getVMForHost(String hostname, String processName) ;


    /**
     *  Returns a deployed Service component that was deployed on the specified VM.
     *  @param serviceID identifies the service definition that was deployed
     *  @param vmDeployedComponent is the deployed VM the service was deployed to.
     *  @return DeployedComponent
     */
    DeployedComponent getDeployedServiceForVM(ServiceComponentDefnID serviceID, VMComponentDefn vmComponent);

    /**
     *  Returns a deployed Service component that was deployed on the specified VM and Host.
     *  @param serviceID identifies the service definition that was deployed
     *  @param vmID is the VM the service was deployed to.
     *  @param hostID is the Host the service was deployed to.
     *  @return DeployedComponent
     */
    DeployedComponent getDeployedServiceForVM(ServiceComponentDefnID serviceID, VMComponentDefnID vmID, HostID hostID);

    /**
     *  Returns a <code>Collection</code> of deployed components that are VM's deployed
     *  on the specified host name
     *  @param name of the Host the vm's were deployed on
     *  @return Collection of deployed components
     */
    Collection getVMsForHost(String name) ;

    /**
     *  Returns a <code>Collection</code> of type <code>DeployeComponent</code> that
     *  represent VMs.
     *  @param id of the Host the vm's were deployed on
     *  @return Collection of deployed components
     */
    Collection getVMsForHost(HostID id) ;

    /**
     * Returns a <code>Collection</code> of type <code>HostID</code> that were deployed for this configuration.
     *  @return Collection of deployed hosts
     */
    Collection getHostIDs();

    /**
     * This method is technically incorrect, because deployed services should
     * be returned for the <i>deployed</i> vm that they are deployed to,
     * <i>not</i> the vm component definition which may itself be deployed
     * many times.
     */
    Collection getDeployedServicesForVM(VMComponentDefnID vmComponentID) ;

    /**
     * Returns a <code>Collection</code> of DeployedComponents that
     * represent services deployed on the indicated deployed VM
     * @param vm component represents the deployed vm for which the services
     * have been deployed.
     * @return Collection of DeployedComponent objects representing deployed
     * services
      */
    Collection getDeployedServicesForVM(VMComponentDefn vm) ;

    /**
     * Returns a <code>Collection</code> of DeployedComponents that
     * represent ServiceComponentDefns deployed on the indicated deployed VM,
     * and originating from the indicated ProductServiceConfig
     * @param vm component represents the deployed vm for which the services
     * have been deployed.
     * @param psc ProductServiceConfig which the desired deployed
     * ServiceComponentDefns belong to
     * @return Collection of DeployedComponent objects representing deployed
     * services
     */
 //   Collection getDeployedServices(VMComponentDefn vm, ProductServiceConfig psc) ;

    /**
     * Returns a Collection of ProductServiceConfig objects which have
     * ServiceComponentDefns which are deployed to the indicated VM.
     * Note that, physically speaking, only ServiceComponentDefns and
     * VMComponentDefns are "deployed", so this is just a convenience
     * method for organizing deployed services by the PSC that originated
     * them, and to then organize PSCs by the vm in which those services
     * are deployed.
     * @param vm component VMComponentDefn representing a deployed
     * VMComponentDefn
     * @return Collection of ProductServiceConfig objects
     */
//    Collection getPSCsForVM(VMComponentDefn vm) ;


	/**
	 * Returns the ProductServiceConfig for the specificed id.
	 * @param pscID is the id for specific ProductServiceConfig
	 * @return ProductServiceConfig to be returned
     */
//	ProductServiceConfig getPSC(ComponentDefnID pscID) ;


    /**
     * Returns all the {@link BasicProductServiceConfig PSC} ID's that
     * contains the indicated {@link ServiceComponentDefnID ServiceComponentDefnID},
     * or returns an emtpy collection if none is found.  This is a potentially expensive
     * method call, and involves iterating through all component defns
     * of this Configuration and looking for the right one.
     * @param serviceDefnID ID of the ServiceComponentDefn, which must exist
     * in this Configuration instance
     * @return Collection of type {@link ProductServiceConfigID PSCID} that contain the
     * ServiceComponentDefnID, or empty collection if none is found
     */
 //   Collection getPSCsForServiceDefn(ServiceComponentDefnID serviceDefnID) ;

    /**
     *  Returns a <code>Collection</code> of component objects that are dependent on the
     *  specified component object.
     *  @param componentObjectID to check for dependencies
     *  @return Collection of dependent components
     */
    Collection getComponentObjectDependencies(BaseID componentObjectID) ;


    /**
     * Returns a <code>Collection</code> of type <code>Host</code> that exist for this configuration.
     *  @return Collection of deployed hosts
     */
    Collection getHosts();
    
    /**
     * Returns a <code>Collection</code> of type <code>ConnectorBinding</code> that exist for this configuration.
     *  @return Collection of deployed hosts
     */
    Collection getConnectorBindings();
    
    /**
     * Returns a <code>Collection</code> of type <code>AuthenticationProvider</code> that exist for this configuration.
     *  @return Collection of authentication providers
     */
    Collection getAuthenticationProviders();
    
    
    /**
     * Returns a <code>AuthenticationProvider</code> for the specified name.
     * @param name
     * @return
     */
    AuthenticationProvider getAuthenticationProvider(String name);

    /**
     * Returns a <code>AuthenticationProvider</code> for the specified componentID.
     * @param componentID
     * @return
     */
    AuthenticationProvider getAuthenticationProvider(ComponentDefnID componentID);
    
    
   /**
     * Returns a <code>ConnectorBinding</code> based on the specified name.
     *  @return ConnectorBinding for that name
     */
    ConnectorBinding getConnectorBinding(String name);
    
    
    /**
     * Returns a <code>ConnectorBinding</code> based on the specified routing ID..
     *  @return ConnectorBinding for that name
     */
    ConnectorBinding getConnectorBindingByRoutingID(String routingID);
       
    
   /**
     * Returns a <code>ConnectorBinding</code> based on the specified compoentID
     *  @return ConnectorBinding for that give component ID
     */
	ConnectorBinding getConnectorBinding(ComponentDefnID componentID);
    	    
    
    /**
     * Returns a <code>Host</code> for the specified host name
     *  @return Host
     */
    Host getHost(String hostName) ;
    
    
    /**
     * Returns a <code>Collection</code> of type <code>ProductServiceConfig</code> that represent
     * all the ProductServiceConfig defined.
     * @return Collection of type <code>ProductServiceConfig</code>
     */
//    Collection getPSCs();
  
     /**
     * Returns a <code>ServiceComponentDefn</code> 
     * @return ServiceComponentDefn 
     */
    ServiceComponentDefn getServiceComponentDefn(ComponentDefnID defnID);
  
  
    /**
    * Returns a <code>ServiceComponentDefn</code> based on the service name. 
    * @return ServiceComponentDefn 
    */  
    ServiceComponentDefn getServiceComponentDefn(String name);
  

     /**
     * Returns a <code>Collection</code> of type <code>ServiceComponentDefn</code> that represent
     * all the ServiceComponentDefn defined.
     * @return Collection of type <code>ServiceComponentDefn</code>
     */
    Collection getServiceComponentDefns();
    
    
     /**
     * Returns a <code>VMComponentDefn</code> 
     * @return VMComponentDefn 
     */
    VMComponentDefn getVMComponentDefn(ComponentDefnID componentID);
    
    
        
     /**
     * Returns a <code>Collection</code> of type <code>VMComponentDefn</code> that represent
     * all the VMComponentDefn defined.
     * @return Collection of type <code>VMComponentDefn</code>
     */
    Collection getVMComponentDefns(); 


    /**
     *  Returns a boolean true if the configuration is deployed for current use.
     *  @return boolean true if the configuration is deployed
     */
    boolean isDeployed();

    /**
     *  Returns a boolean true if the configuration is locked for current modifications.
     *  @return boolean true if the configuration is currently locked.
     */
    boolean isLocked();

    /**
     *  Returns a boolean true if the configuration is released and ready for deployment.
     *  @return boolean true if the configuration is released.
     */
    boolean isReleased();

}


