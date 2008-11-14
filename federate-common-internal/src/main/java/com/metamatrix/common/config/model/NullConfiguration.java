/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.*;

import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.api.exceptions.InvalidArgumentException;
import com.metamatrix.common.config.api.exceptions.InvalidNameException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.config.BasicLogConfiguration;
import com.metamatrix.common.log.config.LogConfigurationException;
import com.metamatrix.common.namedobject.BaseID;

/**
 * <p>This configuration knows nothing.  It is used by 
 * {@link com.metamatrix.common.config.reader.SystemCurrentConfigurationReader}
 * in the case where no system configuration is necessary.  This can be activated
 * by using the -Dmetamatrix.config.none debug flag.</p>
 * 
 * <p>The only method that is actually implemented here is {@link #getProperties}.</p>
 * 
 * @see com.metamatrix.common.config.bootstrap.SystemCurrentConfigBootstrap
 * @see com.metamatrix.common.config.reader.SystemCurrentConfigReader
 */
public class NullConfiguration extends BasicConfiguration {

    
    public NullConfiguration() {
        super(new BasicConfigurationInfo(Configuration.NEXT_STARTUP_ID), Configuration.CONFIG_COMPONENT_TYPE_ID);
    }
    
    /*
     * @see BaseObject#getName()
     */
    public String getName() {
        return "NullConfiguration"; //$NON-NLS-1$
    }
    
    public String getDescription() {
    	return "NullConfiguration Description"; //$NON-NLS-1$
    }

    /*
     * @see Configuration#getInfo()
     */
    public ConfigurationInfo getInfo() {
        return null;
    }

    /*
     * @see ComponentObject#getProperties()
     */
    public Properties getProperties() {
        return System.getProperties();
    }

    /*
     * @see ComponentObject#getProperty(String)
     */
    public String getProperty(String name) {
        return System.getProperty(name);
    }

    /*
     * @see Configuration#getComponentDefns()
     */
    public Map getComponentDefns() {
        return Collections.EMPTY_MAP;
    }

    /*
     * @see Configuration#getComponentDefn(ComponentDefnID)
     */
    public ComponentDefn getComponentDefn(ComponentDefnID componentID) {
        return null;
    }

    /*
     * @see Configuration#getComponentDefnIDs(ComponentTypeID)
     */
    public Collection getComponentDefnIDs(ComponentTypeID componentTypeID) {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see Configuration#getProductServiceConfigIDs(ComponentTypeID)
     * @deprecated
     */
    public Collection getProductServiceConfigIDs(ComponentTypeID productTypeID) {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getDependentPropsForComponent(BaseID)
     */
    public Properties getDependentPropsForComponent(BaseID componentObjectID)
         {
        return System.getProperties();
    }

    /*
     * @see Configuration#getAllPropertiesForComponent(BaseID)
     */
    public Properties getAllPropertiesForComponent(BaseID componentObjectID)
         {
        return System.getProperties();
    }

    /*
     * @see Configuration#getDeployedComponents()
     */
    public Collection getDeployedComponents() {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getDeployedComponents(ComponentDefnID)
     */
    public Collection getDeployedComponents(ComponentDefnID componentDefnID) {
        return Collections.EMPTY_LIST;
    }
    
    public boolean isPSCDeployed(ProductServiceConfigID pscID) {
		return false;
    }
    
    public Collection getPSCsForServiceDefn(ServiceComponentDefnID serviceDefnID)  {
    	return Collections.EMPTY_LIST;
    }    
 
	public Collection getDeployedComponents(ComponentDefnID componentDefnID, ProductServiceConfigID pscID) {
		return Collections.EMPTY_LIST;
	}    
    public DeployedComponent getDeployedComponent(DeployedComponentID deployedComponentID) {
		return null;
    }    

    /*
     * @see Configuration#getDeployedVMForHost(VMComponentDefnID, HostID)
     */
    public VMComponentDefn getVMForHost(
        VMComponentDefnID vmID,
        HostID hostID) {
        return null;
    }
    
    public VMComponentDefn getVMForHost(String hostname, String vmname)   {

        return null;
    }
    

    /*
     * @see Configuration#getDeployedServiceForVM(ServiceComponentDefnID, DeployedComponent)
     */
    public DeployedComponent getDeployedServiceForVM(
        ServiceComponentDefnID serviceID,
        VMComponentDefn vmComponent) {
        return null;
    }

    /*
     * @see Configuration#getDeployedServiceForVM(ServiceComponentDefnID, VMComponentDefnID, HostID)
     */
    public DeployedComponent getDeployedServiceForVM(
        ServiceComponentDefnID serviceID,
        VMComponentDefnID vmID,
        HostID hostID) {
        return null;
    }

    /*
     * @see Configuration#getDeployedVMsForHost(String)
     */
    public Collection getVMsForHost(String name)
        {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getDeployedVMsForHost(HostID)
     */
    public Collection getVMsForHost(HostID id) {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getDeployedHostIDs()
     */
    public Collection getHostIDs() {
        return Collections.EMPTY_LIST;
    }

    /**
     * @see Configuration#getDeployedServicesForVM(VMComponentDefnID)
     * @deprecated
     */
    public Collection getDeployedServicesForVM(VMComponentDefnID vmComponentID)
         {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getDeployedServicesForVM(DeployedComponent)
     */
    public Collection getDeployedServicesForVM(VMComponentDefn vm)
        {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getDeployedServices(DeployedComponent, ProductServiceConfig)
     */
    public Collection getDeployedServices(
        VMComponentDefn vm,
        ProductServiceConfig psc)
         {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getDeployedServicesForVM(String)
     */
    public Collection getDeployedServicesForVM(String deployedVMFullName)
        throws InvalidArgumentException, InvalidNameException {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getPSCsForVM(DeployedComponent)
     */
    public Collection getPSCsForVM(DeployedComponent deployedVM)
        throws InvalidArgumentException {
        return Collections.EMPTY_LIST;
    }
    
    
    public ProductServiceConfig getPSC(ComponentDefnID componentID) {
    	return null;
    }
    
    
   /*
     * @see Configuration#getComponentObjectDependencies(BaseID)
     */
    public Collection getComponentObjectDependencies(BaseID componentObjectID)
         {
        return Collections.EMPTY_LIST;
    }

    /*
     * @see Configuration#getLogConfiguration()
     */
    public LogConfiguration getLogConfiguration() {
        try {
	        return BasicLogConfiguration.createLogConfiguration(System.getProperties());
        } catch(LogConfigurationException e) {
         	return new BasicLogConfiguration();   
        }
    }
    
    public Collection getHosts() {
        return Collections.EMPTY_LIST;
    }
    
    public Collection getConnectorBindings() {
        return Collections.EMPTY_LIST;
    }
	    
    public ConnectorBinding getConnectorBinding(String name) {
    	return null;
    }
    
    
   /**
     * Returns a <code>ConnectorBinding</code> based on the specified compoentID
     *  @return ConnectorBinding for that give component ID
     */
	public ConnectorBinding getConnectorBinding(ComponentDefnID componentID) {
		return null;
	}
    
    public ConnectorBinding getConnectorBindingByRoutingID(String routingID) {
             return null;
     }
    
	    
    
    /**
     * Returns a <code>Host</code> for the specified host name
     *  @return Host
     */
    public Host getHost(String hostName) {
        return null;    
    }
    
    public Collection getResourcePools() {
        return Collections.EMPTY_LIST;        
    }   
    
    /**
     * Returns a <code>ResourceDescriptor</code> for the specified <code>descriptorName</code>
     * @param descriptorName is the name of the resource descriptor to return
     * @return ResourceDescriptor is the descriptor requested
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     */
    public ResourceDescriptor getResourcePool(String poolName)  {
        return null;
    }
    
    public Collection getPSCs() {
        	return Collections.EMPTY_LIST;         
    }   
    
    public VMComponentDefn getVMComponentDefn(ComponentDefnID componentID) {
		return null;
    }    
  
    public ServiceComponentDefn getServiceComponentDefn(ComponentDefnID componentID) {
			return null;
    }     

    public ServiceComponentDefn getServiceComponentDefn(String componentName) {
            return null;
    } 
       
     /**
     * Returns a <code>Collection</code> of type <code>ServiceComponentDefn</code> that represent
     * all the ServiceComponentDefn defined.
     * @return Collection of type <code>ServiceComponentDefn</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductServiceConfig
     */
    public Collection getServiceComponentDefns() {
        	return Collections.EMPTY_LIST;         
    } 
    
     /**
     * Returns a <code>Collection</code> of type <code>VMComponentDefn</code> that represent
     * all the VMComponentDefn defined.
     * @return Collection of type <code>VMComponentDefn</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductServiceConfig
     */
    public Collection getVMComponentDefns() {
        	return Collections.EMPTY_LIST;
         
    }     
    
    
    /*
     * @see Configuration#isDeployed()
     */
    public boolean isDeployed() {
        return false;
    }

    /*
     * @see Configuration#isLocked()
     */
    public boolean isLocked() {
        return false;
    }

    /*
     * @see Configuration#isReleased()
     */
    public boolean isReleased() {
        return false;
    }


    /*
     * @see ComponentObject#getComponentTypeID()
     */
    public ComponentTypeID getComponentTypeID() {
        return null;
    }

    /*
     * @see ComponentObject#isDependentUpon(BaseID)
     */
    public boolean isDependentUpon(BaseID componentObjectId) {
        return false;
    }

    /*
     * @see BaseObject#getID()
     */
    public BaseID getID() {
        return null;
    }

    /*
     * @see BaseObject#getFullName()
     */
    public String getFullName() {
        return "NullConfiguration"; //$NON-NLS-1$
    }
    

    /**
     * Returns the principal who created this type
     * @return String principal name 
     */
    public String getCreatedBy(){
        return ""; //$NON-NLS-1$
    }

    public void setCreatedBy(String createdBy){
        
    }

    /**
     * Returns the Date this type was created
     * @return Date this type was created
     */
    public Date getCreatedDate(){
        return new Date();
    }

    public void setCreatedDate(Date createdDate){
    }

    /**
     * Returns the principal who last modified this type
     * @return String principal name
     */
    public String getLastChangedBy(){
    		return ""; //$NON-NLS-1$
    }

    public void setLastChangedBy(String lastChangedBy){
    }

    /**
     * Returns the Date this type was last changed
     * @return Date this type was last changed
     */
    public Date getLastChangedDate(){
      	return new Date();

    }

    public void setLastChangedDate(Date lastChangedDate){
    }
    
    

    /*
     * @see Comparable#compareTo(Object)
     */
    public int compareTo(Object o) {
        return 0;
    }
    
    public Object clone() { 
        return new NullConfiguration();
    }

}
