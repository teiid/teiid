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

package com.metamatrix.admin.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationInfo;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.model.BasicComponentObject;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.BasicConnectorBinding;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.config.model.BasicServiceComponentDefn;
import com.metamatrix.common.config.model.BasicVMComponentDefn;
import com.metamatrix.common.config.model.ConfigurationVisitor;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.namedobject.BaseID;


/**
 * Fake implementation that creates fake data for testing the Admin API. 
 * @since 4.3
 */
public class FakeConfiguration implements Configuration {
    
    
    /**List<DeployedComponent**/
    public List deployedComponents;
    public List services;
    public Map serviceMap;
    public List connectorbindings;
    protected Map hosts ;
    

    public FakeConfiguration() {
        initDeployedComponents();
    
    }
    
    
    public String getName() {
        return null;
    }

    public ConfigurationInfo getInfo() {
        return null;
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("key1", "value1"); //$NON-NLS-1$ //$NON-NLS-2$
        return properties;
    }

    public String getProperty(String name) {
        return null;
    }

    public Map getComponentDefns() {
        return null;
    }

    public ComponentDefn getComponentDefn(ComponentDefnID componentID) {
        return null;
    }

    public Collection getComponentDefnIDs(ComponentTypeID componentTypeID) {
        return null;
    }

    public Properties getDependentPropsForComponent(BaseID componentObjectID) {
        return null;
    }

    public Properties getAllPropertiesForComponent(BaseID componentObjectID) {
        return null;
    }

    
    
    /**
     * Initialize collection of fake DeployedComponents, for use by this and FakeRuntimeStateAdminHelper. 
     * Creates 3 connector bindings and 3 DQPs.
     * @since 4.3
     */
    public void initDeployedComponents() {
        deployedComponents = new ArrayList();
        hosts = new HashMap();
        serviceMap = new HashMap();
        this.connectorbindings = new ArrayList();
        this.services = new ArrayList();
        
        //Connector Bindings
        ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(false);
        Host h = editor.createHost(Configuration.NEXT_STARTUP_ID, "1.1.1.1"); //$NON-NLS-1$
        
        editor.addProperty(h, HostType.HOST_PHYSICAL_ADDRESS, "1.1.1.1"); //$NON-NLS-1$
        
        hosts.put(h.getID().getName(), h);
        
    //    HostID hostID1 = new HostID("1.1.1.1"); //$NON-NLS-1$
  
        VMComponentDefnID processID1 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, (HostID) h.getID(), "process1");  //$NON-NLS-1$
        DeployedComponentID deployedComponentID1 = new DeployedComponentID("connectorBinding1", Configuration.NEXT_STARTUP_ID, (HostID) h.getID(),  //$NON-NLS-1$
                                                                           processID1); 
        ConnectorBindingID connectorBindingID1 = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, "connectorBinding1"); //$NON-NLS-1$
        BasicConnectorBinding cb = new BasicConnectorBinding(Configuration.NEXT_STARTUP_ID, connectorBindingID1, ConnectorBindingType.CONNECTOR_TYPE_ID);
        
        this.connectorbindings.add(cb);
        BasicDeployedComponent deployedComponent1 = new BasicDeployedComponent(deployedComponentID1, Configuration.NEXT_STARTUP_ID, 
                                                                               (HostID) h.getID(), processID1, 
                                                                          connectorBindingID1, 
                                                                          ConnectorBindingType.CONNECTOR_TYPE_ID);
        deployedComponent1.setDescription("connectorBinding1"); //$NON-NLS-1$
        deployedComponents.add(deployedComponent1);
        
        
  //      HostID hostID2 = new HostID("2.2.2.2"); //$NON-NLS-1$
        Host h2 = editor.createHost(Configuration.NEXT_STARTUP_ID, "2.2.2.2"); //$NON-NLS-1$
        
        editor.addProperty(h2, HostType.HOST_PHYSICAL_ADDRESS, "2.2.2.2"); //$NON-NLS-1$
        
        hosts.put(h2.getID().getName(), h2);
        
        VMComponentDefnID processID2 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, (HostID) h2.getID(), "process2");  //$NON-NLS-1$
        DeployedComponentID deployedComponentID2 = new DeployedComponentID("connectorBinding2", Configuration.NEXT_STARTUP_ID, (HostID) h2.getID(),  //$NON-NLS-1$
                                                                           processID2); 
        ConnectorBindingID connectorBindingID2 = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, "connectorBinding2"); //$NON-NLS-1$
        BasicConnectorBinding cb2 = new BasicConnectorBinding(Configuration.NEXT_STARTUP_ID, connectorBindingID2, ConnectorBindingType.CONNECTOR_TYPE_ID);
        
        this.connectorbindings.add(cb2);

        
        BasicDeployedComponent deployedComponent2 = new BasicDeployedComponent(deployedComponentID2, Configuration.NEXT_STARTUP_ID, 
                                                                               (HostID) h2.getID(), processID2, 
                                                                          connectorBindingID2, 
                                                                          ConnectorBindingType.CONNECTOR_TYPE_ID);
        deployedComponent2.setDescription("connectorBinding2"); //$NON-NLS-1$
        deployedComponents.add(deployedComponent2);
        
        
        
//        HostID hostID3 = new HostID("3.3.3.3"); //$NON-NLS-1$
        Host h3 = editor.createHost(Configuration.NEXT_STARTUP_ID, "3.3.3.3"); //$NON-NLS-1$
        
        editor.addProperty(h3, HostType.HOST_PHYSICAL_ADDRESS, "3.3.3.3"); //$NON-NLS-1$
        
        hosts.put(h3.getID().getName(), h2);
        
        VMComponentDefnID processID3 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, (HostID) h3.getID(), "process3");  //$NON-NLS-1$
        DeployedComponentID deployedComponentID3 = new DeployedComponentID("connectorBinding3", Configuration.NEXT_STARTUP_ID, (HostID) h3.getID(),  //$NON-NLS-1$
                                                                           processID3); 
        ConnectorBindingID connectorBindingID3 = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, "connectorBinding3"); //$NON-NLS-1$
        BasicConnectorBinding cb3 = new BasicConnectorBinding(Configuration.NEXT_STARTUP_ID, connectorBindingID3, ConnectorBindingType.CONNECTOR_TYPE_ID);
        
        this.connectorbindings.add(cb3);

        
        BasicDeployedComponent  deployedComponent3 = new BasicDeployedComponent(deployedComponentID3, Configuration.NEXT_STARTUP_ID, 
                                                                                (HostID) h3.getID(), processID3, 
                                                                          connectorBindingID3, 
                                                                          ConnectorBindingType.CONNECTOR_TYPE_ID);
        deployedComponent3.setDescription("connectorBinding3"); //$NON-NLS-1$
        deployedComponents.add(deployedComponent3);
        
        
        
        //DQPS        
        DeployedComponentID deployedComponentID1A = new DeployedComponentID("dqp1", Configuration.NEXT_STARTUP_ID, (HostID) h.getID(),  //$NON-NLS-1$
                                                                           processID1); 
        ServiceComponentDefnID defnID1A = new ServiceComponentDefnID(Configuration.NEXT_STARTUP_ID, "dqp1"); //$NON-NLS-1$
        BasicServiceComponentDefn sdfn = new BasicServiceComponentDefn(Configuration.NEXT_STARTUP_ID, defnID1A, new ComponentTypeID("QueryService"));
                
                this.services.add(sdfn);
                serviceMap.put(defnID1A.getFullName(), sdfn);
       		
        
        BasicDeployedComponent deployedComponent1A = new BasicDeployedComponent(deployedComponentID1A, Configuration.NEXT_STARTUP_ID, 
                                                                                (HostID) h.getID(), processID1, 
                                                                          defnID1A, 
                                                                          new ComponentTypeID("QueryService")); //$NON-NLS-1$
        deployedComponent1A.setDescription("dqp1"); //$NON-NLS-1$
        deployedComponents.add(deployedComponent1A);
        
        
        DeployedComponentID deployedComponentID2A = new DeployedComponentID("dqp2", Configuration.NEXT_STARTUP_ID, (HostID) h2.getID(),  //$NON-NLS-1$
                                                                           processID2); 
        ServiceComponentDefnID defnID2A = new ServiceComponentDefnID(Configuration.NEXT_STARTUP_ID, "dqp2"); //$NON-NLS-1$
        BasicServiceComponentDefn sdfn2 = new BasicServiceComponentDefn(Configuration.NEXT_STARTUP_ID, defnID2A, new ComponentTypeID("QueryService"));
        
        this.services.add(sdfn2);
        serviceMap.put(defnID2A.getFullName(), sdfn2);

        
        BasicDeployedComponent deployedComponent2A = new BasicDeployedComponent(deployedComponentID2A, Configuration.NEXT_STARTUP_ID, 
                                                                                (HostID) h2.getID(), processID2, 
                                                                          defnID2A, 
                                                                          new ComponentTypeID("QueryService")); //$NON-NLS-1$
        deployedComponent2A.setDescription("dqp2"); //$NON-NLS-1$
        deployedComponents.add(deployedComponent2A);
        
                
        DeployedComponentID deployedComponentID3A = new DeployedComponentID("dqp3", Configuration.NEXT_STARTUP_ID, (HostID) h3.getID(),  //$NON-NLS-1$
                                                                           processID3); 
        ServiceComponentDefnID defnID3A = new ServiceComponentDefnID(Configuration.NEXT_STARTUP_ID, "dqp3"); //$NON-NLS-1$
       BasicServiceComponentDefn sdfn3 = new BasicServiceComponentDefn(Configuration.NEXT_STARTUP_ID, defnID3A, new ComponentTypeID("QueryService"));
        
        this.services.add(sdfn3);
        serviceMap.put(defnID3A.getFullName(), sdfn3);    
        
        BasicDeployedComponent  deployedComponent3A = new BasicDeployedComponent(deployedComponentID3A, Configuration.NEXT_STARTUP_ID, 
                                                                                 (HostID) h3.getID(), processID3, 
                                                                          defnID3A, 
                                                                          new ComponentTypeID("QueryService")); //$NON-NLS-1$
        deployedComponent3A.setDescription("dqp3"); //$NON-NLS-1$
        deployedComponents.add(deployedComponent3A);
    }

    
    
    /**
     * Return collection of fake DeployedComponents 
     * Returns "connectorBinding1" and "connectorBinding2";
     * "dqp1" and "dqp2".
     * @since 4.3
     */
    public Collection getDeployedComponents() {
        List results = new ArrayList();
        
        results.add(deployedComponents.get(0));       
        results.add(deployedComponents.get(1));
        results.add(deployedComponents.get(2));
        results.add(deployedComponents.get(3));       
        results.add(deployedComponents.get(4));
        results.add(deployedComponents.get(5));
        
        
        return results;
    }

    public Collection getDeployedComponents(ComponentDefnID componentDefnID) {
        return null;
    }

//    public Collection getDeployedComponents(ComponentDefnID componentDefnID,
//                                            ProductServiceConfigID pscID) {
//        return null;
//    }

//    public boolean isPSCDeployed(ProductServiceConfigID pscID) {
//        return false;
//    }

    public DeployedComponent getDeployedComponent(DeployedComponentID deployedComponentID) {
        return null;
    }

    public VMComponentDefn getVMForHost(VMComponentDefnID vmID,
                                        HostID hostID) {
        return null;
    }

    public VMComponentDefn getVMForHost(String hostname,
                                        String processName) {
        return null;
    }

    public DeployedComponent getDeployedServiceForVM(ServiceComponentDefnID serviceID,
                                                     VMComponentDefn vmComponent) {
        return null;
    }

    public DeployedComponent getDeployedServiceForVM(ServiceComponentDefnID serviceID,
                                                     VMComponentDefnID vmID,
                                                     HostID hostID) {
        return null;
    }

    public Collection getVMsForHost(String name) {
        return null;
    }

    public Collection getVMsForHost(HostID id) {
        return null;
    }

    public Collection getHostIDs() {
        return null;
    }

    public Collection getDeployedServicesForVM(VMComponentDefnID vmComponentID) {
        return null;
    }

    public Collection getDeployedServicesForVM(VMComponentDefn vm) {
        return null;
    }


    public Collection getPSCsForVM(VMComponentDefn vm) {
        return null;
    }

    public Collection getPSCsForServiceDefn(ServiceComponentDefnID serviceDefnID) {
        return null;
    }

    public Collection getComponentObjectDependencies(BaseID componentObjectID) {
        return null;
    }

    public Collection getHosts() {
        return hosts.values();
    }

    public Collection getAuthenticationProviders() {
        return null;
    }

    public Collection getConnectorBindings() {
       List results = new ArrayList();
        
       results.addAll(this.connectorbindings);
       return results;
    }

    /**
     * Return fake ConnectorBinding, based on the specified name. 
     * Returns "connectorBinding1" and "connectorBinding2".
     * @see com.metamatrix.common.config.api.Configuration#getResourcePool(java.lang.String)
     * @since 4.3
     */
    public ConnectorBinding getConnectorBinding(String name) {
        if ("connectorBinding1".equals(name)) { //$NON-NLS-1$
            ConnectorBindingID connectorBindingID = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, name);
            BasicConnectorBinding connectorBinding = new BasicConnectorBinding(Configuration.NEXT_STARTUP_ID, connectorBindingID, 
                                                                          ConnectorBindingType.CONNECTOR_TYPE_ID); 
            connectorBinding.setRoutingUUID(name+"uuid"); //$NON-NLS-1$
            return connectorBinding;
        } else if ("connectorBinding2".equals(name)) { //$NON-NLS-1$
            ConnectorBindingID connectorBindingID = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, name);
            BasicConnectorBinding connectorBinding = new BasicConnectorBinding(Configuration.NEXT_STARTUP_ID, connectorBindingID, 
                                                                          ConnectorBindingType.CONNECTOR_TYPE_ID); 
            connectorBinding.setRoutingUUID(name+"uuid"); //$NON-NLS-1$

            BasicComponentObject target = (BasicComponentObject) connectorBinding;

            target.addProperty("prop1", "value1");
            target.addProperty("prop2", "value2");
            

            return connectorBinding;            
        } else if ("connectorBinding3".equals(name)) { //$NON-NLS-1$
            ConnectorBindingID connectorBindingID = new ConnectorBindingID(Configuration.NEXT_STARTUP_ID, name);
            BasicConnectorBinding connectorBinding = new BasicConnectorBinding(Configuration.NEXT_STARTUP_ID, connectorBindingID, 
                                                                          ConnectorBindingType.CONNECTOR_TYPE_ID);
            connectorBinding.setRoutingUUID(name+"uuid"); //$NON-NLS-1$
            return connectorBinding;            
        }
        
        return null;
        
    }

    
    /**
     * Return fake ConnectorBinding for testing, based on the specified routingID.
     * Returns "connectorBinding1" and "connectorBinding2" 
     * @see com.metamatrix.common.config.api.Configuration#getConnectorBindingByRoutingID(java.lang.String)
     * @since 4.3
     */
    public ConnectorBinding getConnectorBindingByRoutingID(String routingID) {
        
        return this.getConnectorBinding(routingID);
    }

    public AuthenticationProvider getAuthenticationProvider(ComponentDefnID componentID) {
        return null;
    }
    
    public AuthenticationProvider getAuthenticationProvider(String name) {
        return null;
    }

    public ConnectorBinding getConnectorBinding(ComponentDefnID componentID) {
        return null;
    }

    public Host getHost(String hostName) {
        return (Host) hosts.get(hostName);
    }

    public Collection getPSCs() {
        return null;
    }

    public ServiceComponentDefn getServiceComponentDefn(ComponentDefnID defnID) {
        return (ServiceComponentDefn) serviceMap.get(defnID.getFullName());
    }

    public ServiceComponentDefn getServiceComponentDefn(String name) {
    	 return (ServiceComponentDefn) serviceMap.get(name);
    }

    public Collection getServiceComponentDefns() {
        List results = new ArrayList();
        
        results.addAll(this.services);
        return results;

    }

    public VMComponentDefn getVMComponentDefn(ComponentDefnID componentID) {
        return null;
    }

    /**
     * Return fake VMComponentDefns for testing.
     * Returns "process1" and "process2" 
     * @see com.metamatrix.common.config.api.Configuration#getVMComponentDefns()
     * @since 4.3
     */
    public Collection getVMComponentDefns() {
        List defns = new ArrayList();
        
        HostID hostID1 = new HostID("1.1.1.1"); //$NON-NLS-1$
        VMComponentDefnID defnID1 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, hostID1, "process1");  //$NON-NLS-1$
        VMComponentDefn defn1 = new BasicVMComponentDefn(Configuration.NEXT_STARTUP_ID, hostID1, defnID1, new ComponentTypeID("process1"));  //$NON-NLS-1$
        defns.add(defn1);
        
        HostID hostID2 = new HostID("2.2.2.2"); //$NON-NLS-1$
        VMComponentDefnID defnID2 = new VMComponentDefnID(Configuration.NEXT_STARTUP_ID, hostID2, "process2");  //$NON-NLS-1$
        VMComponentDefn defn2 = new BasicVMComponentDefn(Configuration.NEXT_STARTUP_ID, hostID2, defnID2, new ComponentTypeID("process2"));  //$NON-NLS-1$
        defns.add(defn2);
        
        
        return defns;
    }

    public LogConfiguration getLogConfiguration() {
        return null;
    }

    public boolean isDeployed() {
        return false;
    }

    public boolean isLocked() {
        return false;
    }

    public boolean isReleased() {
        return false;
    }

    public ComponentTypeID getComponentTypeID() {
        return null;
    }

    public String getDescription() {
        return null;
    }

    public String getCreatedBy() {
        return null;
    }

    public Date getCreatedDate() {
        return null;
    }

    public String getLastChangedBy() {
        return null;
    }

    public Date getLastChangedDate() {
        return null;
    }

    public boolean isDependentUpon(BaseID componentObjectId) {
        return false;
    }

    public void accept(ConfigurationVisitor visitor) {
    }

    public BaseID getID() {
        return null;
    }

    public String getFullName() {
        return null;
    }

    public int compareTo(Object obj) {
        return 0;
    }
    
    public Object clone() {
        return new FakeConfiguration();
    }

}
