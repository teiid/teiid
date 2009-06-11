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

package com.metamatrix.platform.admin.api.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.HostControllerRegistryBinding;
import com.metamatrix.platform.registry.ProcessRegistryBinding;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.controller.ServicePropertyNames;
import com.metamatrix.server.HostManagement;

/**
 * This class is a container for ServiceRegistryBinding objects for
 * all the services running in this VM
 */
public class SystemStateBuilder {

    private Collection deployedComponents;
    private Configuration config;
    ClusteredRegistryState registry;
    HostManagement hostManagement;
    
    /**
     * Create a new instance of VMRegistryBinding.
     *
     * @param vmID Identifies VMController binding represents
     * @param vmController VMController implementation
     * @param hostName Name of host VM is running on
     */
    public SystemStateBuilder(ClusteredRegistryState registry, HostManagement hostManagement) throws Exception {
    	this.registry = registry;
    	this.hostManagement = hostManagement;
        config = CurrentConfiguration.getInstance().getConfiguration();
        deployedComponents = config.getDeployedComponents();
        Collection vms = config.getVMComponentDefns();
        deployedComponents.addAll(vms);
    }

    public SystemState getSystemState() throws Exception {

        List<HostControllerRegistryBinding> allHosts = this.registry.getHosts();
        List hosts = new ArrayList();
        List hostIDs = new ArrayList();

        // Create a new HostData object for each running host.
        for (HostControllerRegistryBinding host:allHosts) {
            hosts.add(createHostData(host));
            hostIDs.add(new HostID(host.getHostName()));
        }

        // get a list of deployed hosts and remove all running hosts
        Collection deployedHosts = config.getHostIDs();
        deployedHosts.removeAll(hostIDs);
        Iterator dIter = deployedHosts.iterator();

        // Create a new HostData object for each non-running deployed host
        while (dIter.hasNext()) {
            HostID hostID = (HostID) dIter.next();
            hosts.add(createHostData(hostID));
        }

        return new SystemState(hosts);
    }

    /**
     * Create a HostData object from the hostBinding.
     */
    private HostData createHostData(HostControllerRegistryBinding host) throws Exception {

    	String hostName = host.getHostName();
    	
        List vmBindings = this.registry.getVMs(hostName);
        List processes = new ArrayList();
        Collection deployedVMs = null;
        try {
            deployedVMs = config.getVMsForHost(hostName);
        } catch (Exception e) {
            deployedVMs = new ArrayList();
        }
        Iterator iter = vmBindings.iterator();

        // loop thru all vm's for the host and create a ProcessData object for each.
        while (iter.hasNext()) {
            ProcessRegistryBinding vmBinding = (ProcessRegistryBinding) iter.next();
            processes.add(createProcessData(vmBinding));
            deployedVMs.remove(vmBinding.getDeployedComponent());
        }

        // now create a processData object for each deployed process that is not running
        iter = deployedVMs.iterator();
        while (iter.hasNext()) {
            VMComponentDefn dCmp = (VMComponentDefn) iter.next();
            processes.add(createProcessData(dCmp));
        }

        // create and return HostData object
        HostID hostID =  new HostID(hostName);
        boolean deployed = config.getHostIDs().contains(hostID);

        boolean running = isHostRunning(hostName);
        return new HostData(hostName, processes, deployed, running, host.getProperties());
    }

    /**
     * Create a HostData object from the hostID
     * This method is called for a deployedHost that is not
     * currently in the registry.
     */
    private HostData createHostData(HostID hostID) throws Exception {

        Collection deployedVMs = null;
        try {
            deployedVMs = config.getVMsForHost(hostID);
        } catch (Exception e) {
            deployedVMs = new ArrayList();
        }
        List processes = new ArrayList();

        Iterator iter = deployedVMs.iterator();
        while (iter.hasNext()) {
            VMComponentDefn deployedComponent = (VMComponentDefn) iter.next();
            processes.add(createProcessData(deployedComponent));
        }

        boolean running = isHostRunning(hostID.getFullName());
        return new HostData(hostID.getFullName(), processes, true, running, new Properties());
    }
    
    protected boolean isHostRunning(String hostName) {
    	return this.hostManagement.ping(hostName);
    }


    /**
     * Create a ProcessData object for the vmBinding.
     */
    private ProcessData createProcessData(ProcessRegistryBinding vmBinding) {

        // if this vm is not deployed (appServer VM) then
        // create an empty ProcessData and return.
        if (vmBinding.getDeployedComponent() == null) {
            return new ProcessData(	vmBinding.getHostName(),
            						vmBinding.getProcessName(),
            						String.valueOf(vmBinding.getPort()),
            						null,    // deployed component id
            						Collections.EMPTY_LIST,
            						false, // not deployed
            						true); // registered
        }

        // ServiceBindings for vm
        List bindings = this.registry.getServiceBindings(vmBinding.getHostName(), vmBinding.getProcessName()); 

         Set<ServiceComponentDefnID> serviceKeys = new HashSet<ServiceComponentDefnID>(bindings.size());
        Set<ServiceData> serviceDatas = new HashSet<ServiceData>(bindings.size());

        // get all running services from registry.
        // for each create ServiceData object and add to the appropriate list
        Iterator iter = bindings.iterator();
        while (iter.hasNext()) {
            ServiceRegistryBinding svcBinding = (ServiceRegistryBinding) iter.next();

            // create a ServiceData object and add to list
            // Note: createServiceData removes svc from global deployedServicesList.
            serviceKeys.add(svcBinding.getDeployedComponent().getServiceComponentDefnID());
            serviceDatas.add(createServiceData(svcBinding));
         }

        // now get all deployed services for this vm that are not running.
        List cmpList = new ArrayList(deployedComponents);
        iter = cmpList.iterator();
        while (iter.hasNext()) {

            Object o = iter.next();
            if (o instanceof DeployedComponent) {
                
                DeployedComponent dCmp = (DeployedComponent) o;
                // check to see if this component belongs to this vm
                if (vmBinding.getDeployedComponent().getID().equals(dCmp.getVMComponentDefnID()) &&
                    dCmp.getServiceComponentDefnID() != null &&
                    ! serviceKeys.contains(dCmp.getServiceComponentDefnID() ) ){
    
                    String essentialStr = dCmp.getProperty(ServicePropertyNames.SERVICE_ESSENTIAL);
                    boolean essential = false;
                    if (essentialStr != null && essentialStr.trim().length() != 0) {
                        essential = Boolean.valueOf(essentialStr).booleanValue();
                    }
                    
                    serviceKeys.add(dCmp.getServiceComponentDefnID());
                    serviceDatas.add(new ServiceData(null, dCmp.getComponentTypeID().getFullName(),
                            null, dCmp.getServiceComponentDefnID(),
                            dCmp.getName(), null,
                            ServiceState.STATE_NOT_REGISTERED, new Date(), essential, true, false, null));

    
//                    svcMap.put(dCmp.getServiceComponentDefnID(), new ServiceData(null, dCmp.getComponentTypeID().getFullName(),
//                                             null, dCmp.getServiceComponentDefnID(),
//                                             dCmp.getName(), null,
//                                             ServiceState.STATE_NOT_REGISTERED, new Date(), essential, true, false, null)); // deployed, not registered.
    
                    deployedComponents.remove(dCmp);
                }
            }
        }
        // determine if process/vm is deployed
        VMComponentDefn dc = vmBinding.getDeployedComponent();
        boolean deployed = deployedComponents.contains(dc);

        if (deployed) {
            deployedComponents.remove(dc);
        }

        
        return new ProcessData(	vmBinding.getHostName(),
        						vmBinding.getProcessName(), 
        						vmBinding.getPort(),
        						(VMComponentDefnID) vmBinding.getDeployedComponent().getID(),
        						serviceDatas,  
        						deployed, true);
    }


    /**
     * Create ProcessData object from the DeployedComponent.
     */
    private ProcessData createProcessData(VMComponentDefn deployedVM) {

        // get deployed services for this vm
        Collection deployedServices = null;
        try {
            deployedServices = config.getDeployedServicesForVM(deployedVM);
        } catch (Exception e) {
            deployedServices = new ArrayList();
        }

        // Map of PscID -> List of ServiceData objects
        Collection svcList =  new ArrayList();

        Iterator iter = deployedServices.iterator();
        while (iter.hasNext()) {
            DeployedComponent dCmp = (DeployedComponent) iter.next();
//            ProductServiceConfigID id = dCmp.getProductServiceConfigID();
//            if (id == null) {
//                id = new ProductServiceConfigID("Default PSC"); //$NON-NLS-1$
//            }
//            List list = (List) pscMap.get(id);
//            if (list == null) {
//                list = new ArrayList();
//                pscMap.put(id, list);
//            }
            String essentialStr = dCmp.getProperty(ServicePropertyNames.SERVICE_ESSENTIAL);
            boolean essential = false;
            if (essentialStr != null && essentialStr.trim().length() != 0) {
                essential = Boolean.valueOf(essentialStr).booleanValue();
            }
            svcList.add(new ServiceData(null, dCmp.getComponentTypeID().getName(),
                                     null, dCmp.getServiceComponentDefnID(),
                                     dCmp.getName(), null,
                                     ServiceState.STATE_NOT_REGISTERED, new Date(), essential, true, false, null)); // deployed, not registered
        }


        // ok we now have a map of pscID's -> list of ServiceData objects
        // create a list of psc's
//        List pscList = new ArrayList();
//        Iterator pscIter = pscMap.keySet().iterator();
//        String processName = deployedVM.getName();
//
//        while (pscIter.hasNext()) {
//            ProductServiceConfigID pId = (ProductServiceConfigID) pscIter.next();
//            List sList = (List) pscMap.get(pId);
//            pscList.add(createPSCData(pId, sList, processName));
//        }

        deployedComponents.remove(deployedVM);

        String hostName = deployedVM.getHostID().getName();

        return new ProcessData(	hostName,
        						deployedVM.getName(), deployedVM.getPort(),
                               (VMComponentDefnID)deployedVM.getID(),
                               svcList,  
                               true, false); // deployed, not registered
    }

    /**
     * Create PSCData object.
     */
//    private PSCData createPSCData(ProductServiceConfigID pscID, List services, String processName) {
//        return new PSCData(pscID, services, processName);
//    }

    /**
     * Create ServiceData object from the serviceBinding
     */
    private ServiceData createServiceData(ServiceRegistryBinding serviceBinding) {

        ServiceID id = serviceBinding.getServiceID();
        String serviceName = serviceBinding.getServiceType();
        String instanceName = serviceBinding.getInstanceName();
        ComponentDefnID defnID = serviceBinding.getDeployedComponent().getServiceComponentDefnID();
        DeployedComponent deployedComponent = serviceBinding.getDeployedComponent();
        int state = serviceBinding.getCurrentState();
        Date stateDate = serviceBinding.getStateChangeTime();
        boolean essential = serviceBinding.isEssential();
        Collection queues = serviceBinding.getQueueNames();

        boolean deployed = deployedComponents.contains(deployedComponent);

        if (deployed) {
            deployedComponents.remove(deployedComponent);
        }

        return new ServiceData(id,serviceName,instanceName,defnID,deployedComponent.getName(),queues,state,stateDate,essential, deployed, true, serviceBinding.getInitException()); // registered
    }

}

