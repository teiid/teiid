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

package com.metamatrix.admin.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.metamatrix.admin.api.objects.AdminObject;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationLockException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.messaging.NoOpMessageBus;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.common.pooling.api.ResourcePoolStatistics;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.impl.BasicPoolStatistic;
import com.metamatrix.common.pooling.impl.BasicResourcePoolStatistics;
import com.metamatrix.common.pooling.impl.statistics.SumStat;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ResourcePoolStats;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper;
import com.metamatrix.platform.admin.apiimpl.runtime.ResourcePoolStatsImpl;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.platform.vm.controller.VMControllerIDImpl;
import com.metamatrix.platform.vm.controller.VMStatistics;
import com.metamatrix.server.connector.service.ConnectorService;
import com.metamatrix.server.query.service.QueryService;


/**
 * Fake implementation that creates fake data for testing the Admin API. 
 * @since 4.3
 */
public class FakeRuntimeStateAdminAPIHelper extends RuntimeStateAdminAPIHelper {
    
    private FakeConfiguration configuration = new FakeConfiguration();
    
    /**Set<String>  Contains ServiceID.toString() of stopped services.*/
    protected static Set stoppedServices = new HashSet();
    /**Set<String>  Contains ServiceID.toString() of restarted services.*/
    protected static Set restartedServices = new HashSet();

    /**Set<String>  Contains hostname of stopped hosts.*/
    protected static Set stoppedHosts = new HashSet();
    /**Set<String>  Contains hostname of started hosts*/
    protected static Set startedHosts = new HashSet();

    /**Set<String>  Contains VMControllerID.toString() of stopped processes.*/
    protected static Set stoppedProcesses = new HashSet();
    /**Set<String>  Contains "hostName.processname" of started processes*/
    protected static Set startedProcesses = new HashSet();

    protected static boolean shutdownSystem = false;
    protected static boolean synchronizeSystem = false;
    
    protected static void clearState() {
        stoppedServices.clear();
        restartedServices.clear();
        stoppedHosts.clear();
        startedHosts.clear();
        stoppedProcesses.clear();
        startedProcesses.clear();
        
        shutdownSystem = false;
        synchronizeSystem = false;
    }
    
    
    
    public FakeRuntimeStateAdminAPIHelper(ClusteredRegistryState registry) {
    	super(registry);
    }

    public void bounceServer() throws MetaMatrixComponentException {
    }

    public List getHosts() throws MetaMatrixComponentException {
        return null;
    }

    public List getProcesses() throws MetaMatrixComponentException {
        return null;
    }

    public Collection getResourceDescriptors() throws ResourcePoolException,
                                                                         MetaMatrixComponentException {
        return null;
    }
    
    /**
     * Return Collection of fake ResourceDescriptors for testing. 
     * Returns "pool1" and "pool2".
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper#getResourcePoolStatistics(com.metamatrix.platform.registry.MetaMatrixRegistry)
     * @since 4.3
     */
    public Collection getResourcePoolStatistics() throws MetaMatrixComponentException,
                                                                            ResourcePoolException {
        
        List statsList = new ArrayList();
        
        ResourcePool pool1 = new FakeTestResourcePool();
        ResourceDescriptor resourceDescriptor1 = configuration.getResourcePool("pool1"); //$NON-NLS-1$
        pool1.init(resourceDescriptor1);
        ResourcePoolStatistics statistics1 = new BasicResourcePoolStatistics(pool1);
        statistics1.addStatistic(new SumStat("stat1", "stat1", "stat1", BasicPoolStatistic.SUM_AGGREGATE_TYPE)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        statistics1.addStatistic(new SumStat("stat2", "stat2", "stat2", BasicPoolStatistic.SUM_AGGREGATE_TYPE)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        ResourcePoolStats stats1 = new ResourcePoolStatsImpl(statistics1, (ResourceDescriptorID) resourceDescriptor1.getID(), 
                                                             "1.1.1.1", "process1",  //$NON-NLS-1$ //$NON-NLS-2$ 
                                                             new ArrayList());
        statsList.add(stats1);
        
        
        
        ResourcePool pool2 = new FakeTestResourcePool();
        ResourceDescriptor resourceDescriptor2 = configuration.getResourcePool("pool2"); //$NON-NLS-1$
        pool2.init(resourceDescriptor2);
        ResourcePoolStatistics statistics2 = new BasicResourcePoolStatistics(pool2);
        ResourcePoolStats stats2 = new ResourcePoolStatsImpl(statistics2, (ResourceDescriptorID) resourceDescriptor2.getID(),
                                                             "2.2.2.2", "process2", //$NON-NLS-1$ //$NON-NLS-2$
                                                             new ArrayList()); 
        statsList.add(stats2);

        
        return statsList;        
    }
    
    /**
     * Return fake ServiceRegistryBinding for testing, based on the specified ServiceID.
     * Returns "connectorBinding2" and "connectorBinding3"; "dqp2" and "dqp3"
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper#getResourcePoolStatistics(com.metamatrix.platform.registry.MetaMatrixRegistry)
     * @since 4.3
     */
    public ServiceRegistryBinding getServiceBinding(ServiceID serviceID) throws ResourceNotBoundException {

        List deployedComponents = configuration.deployedComponents; 

        
        if (serviceID.getID() == 2) {
            DeployedComponent deployedComponent = (DeployedComponent) deployedComponents.get(1);
            
            ServiceRegistryBinding binding = new ServiceRegistryBinding(serviceID, null, ConnectorService.SERVICE_NAME,
                                                                        "connectorBinding2", ConnectorBindingType.COMPONENT_TYPE_NAME, //$NON-NLS-1$
                                                                        "connectorBinding2", "2.2.2.2", deployedComponent, null,   //$NON-NLS-1$ //$NON-NLS-2$ 
                                                                        AbstractService.STATE_CLOSED,
                                                                        new Date(), false, new NoOpMessageBus());
            return binding;
        } else if (serviceID.getID() == 3) {
            DeployedComponent deployedComponent = (DeployedComponent) deployedComponents.get(2);

            ServiceRegistryBinding binding = new ServiceRegistryBinding(serviceID, null, ConnectorService.SERVICE_NAME,
                                                                        "connectorBinding3", ConnectorBindingType.COMPONENT_TYPE_NAME, //$NON-NLS-1$
                                                                        "connectorBinding3", "3.3.3.3", deployedComponent, null,  //$NON-NLS-1$ //$NON-NLS-2$
                                                                        AbstractService.STATE_CLOSED,
                                                                        new Date(), false, new NoOpMessageBus());                                                                        
            return binding;
            
            
        } else if (serviceID.getID() == 5) {
            DeployedComponent deployedComponent = (DeployedComponent) deployedComponents.get(4);
            
            ServiceRegistryBinding binding = new ServiceRegistryBinding(serviceID, null, QueryService.SERVICE_NAME,
                                                                        "dqp2", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
                                                                        "dqp2", "2.2.2.2", deployedComponent, null, //$NON-NLS-1$ //$NON-NLS-2$ 
                                                                        AbstractService.STATE_CLOSED,
                                                                        new Date(),  
                                                                        false, new NoOpMessageBus()); 
            return binding;
        } else if (serviceID.getID() == 6) {
            DeployedComponent deployedComponent = (DeployedComponent) deployedComponents.get(5);

            ServiceRegistryBinding binding = new ServiceRegistryBinding(serviceID, null, QueryService.SERVICE_NAME,
                                                                        "dqp3", "QueryService", //$NON-NLS-1$ //$NON-NLS-2$
                                                                        "dqp3", "3.3.3.3", deployedComponent, null, //$NON-NLS-1$ //$NON-NLS-2$ 
                                                                        AbstractService.STATE_CLOSED,
                                                                        new Date(),  
                                                                        false, new NoOpMessageBus()); 
            return binding;
            
            
        }
        
        
        return null;
    }

    /**
     * Returns Collection of fake WorkerPoolStats for testing, based on the specified ServiceID. 
     * Returns "connectorBinding2" and "connectorBinding3"; "dqp2" and "dqp3"
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper#getServiceQueueStatistics(com.metamatrix.platform.registry.MetaMatrixRegistry, com.metamatrix.platform.service.api.ServiceID)
     * @since 4.3
     */
    public Collection getServiceQueueStatistics(ServiceID serviceID) throws MetaMatrixComponentException {
        
        long id = serviceID.getID();
        
        if (id == 2 || id == 3 || id == 5 || id == 6) {
            List results = new ArrayList();
            WorkerPoolStats stats = new WorkerPoolStats();
            stats.name = "pool"; //$NON-NLS-1$
            stats.queued = (int) id;
            stats.totalSubmitted = (int) id;
            
            results.add(stats);
            return results;
        } 
        
        return null;
    }

    
    /**
     * Return List of fake ServiceIDs for testing. 
     * Returns "connectorBinding2" and "connectorBinding3"; "dqp2" and "dqp3".
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper#getResourcePoolStatistics(com.metamatrix.platform.registry.MetaMatrixRegistry)
     * @since 4.3
     */
    public List getServices() throws MetaMatrixComponentException {
        List results = new ArrayList();
        
        VMControllerID vmControllerID2 = new VMControllerIDImpl(2, "2.2.2.2"); //$NON-NLS-1$
        ServiceID serviceID2 = new ServiceID(2, vmControllerID2);
        results.add(serviceID2);
        
        VMControllerID vmControllerID3 = new VMControllerIDImpl(3, "3.3.3.3"); //$NON-NLS-1$
        ServiceID serviceID3 = new ServiceID(3, vmControllerID3);
        results.add(serviceID3);
        
        
        ServiceID serviceID2A = new ServiceID(5, vmControllerID2);
        results.add(serviceID2A);
        
        ServiceID serviceID3A = new ServiceID(6, vmControllerID3);
        results.add(serviceID3A);

        
        return results;
    }

    
    /**
     * Returns fake SystemState with fake HostDatas.
     * Returns "2.2.2.2" and "3.3.3.3"; "process2" and "process3"
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper#getSystemState()
     * @since 4.3
     */
    public synchronized SystemState getSystemState() throws MetaMatrixComponentException {
        
        List hosts = new ArrayList();
        
        List processes2 = new ArrayList();   
        VMControllerID vmControllerID1 = new VMControllerIDImpl(2, "2.2.2.2"); //$NON-NLS-1$
        ProcessData process2 = new ProcessData(vmControllerID1, null, "2.2.2.2", new ArrayList(), "process2", "31000", true, true); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        processes2.add(process2);        
        HostData host2 = new HostData("2.2.2.2", processes2, true, true); //$NON-NLS-1$
        hosts.add(host2);

        List processes3 = new ArrayList();        
        VMControllerID vmControllerID3 = new VMControllerIDImpl(3, "3.3.3.3"); //$NON-NLS-1$
        ProcessData process3 = new ProcessData(vmControllerID3, null, "3.3.3.3", new ArrayList(), "process3", "31001", true, true); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        processes3.add(process3);        
        HostData host3 = new HostData("3.3.3.3", processes3, true, true); //$NON-NLS-1$
        hosts.add(host3);
        
        
        return new SystemState(hosts);
        
    }
    

    /**
     * Returns fake SystemState with fake HostDatas.
     * Returns "process2" and "process3"
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper#getSystemState()
     * @since 4.3
     */
    public VMStatistics getVMStatistics(VMControllerID vmID) throws MetaMatrixComponentException {
    
        if (vmID.getID() == 2) {
            VMStatistics statistics = new VMStatistics();    
            statistics.freeMemory = 2;
            statistics.threadCount = 2;
            statistics.socketListenerStats.sockets = 2;
            statistics.processPoolStats.name = "pool"; //$NON-NLS-1$
            statistics.processPoolStats.queued = 2;
            
            return statistics;
        } else if (vmID.getID() == 3) {
            VMStatistics statistics = new VMStatistics();
            statistics.freeMemory = 3;
            statistics.threadCount = 3;
            statistics.socketListenerStats.sockets = 3;
            statistics.processPoolStats.name = "pool"; //$NON-NLS-1$
            statistics.processPoolStats.queued = 3;

            return statistics;
        } else {
            return null;
        }
    }

    
    
    public boolean isSystemStarted() throws MetaMatrixComponentException {
        return true;
    }

    public void restartService(ServiceID serviceID) throws MetaMatrixComponentException {
        
        restartedServices.add(serviceID.toString());
    }

    public void setLogConfiguration(Configuration config,
                                    LogConfiguration logConfig,
                                    List actions,
                                    String principalName) throws ConfigurationLockException,
                                                         ConfigurationException,
                                                         ServiceException,
                                                         MetaMatrixComponentException {

    }

    public synchronized void shutdownServer() throws MetaMatrixComponentException {
        shutdownSystem = true;
    }

    public void startDeployedService(ServiceComponentDefnID serviceID,
                                     VMControllerID vmID) throws MetaMatrixComponentException {
    }

    public void startHost(String host) throws MetaMatrixComponentException {
        startedHosts.add(host);
    }

    public void startProcess(String host,
                             String process) throws MetaMatrixComponentException {
        startedProcesses.add(host+ AdminObject.DELIMITER +process); 
    }

    public void stopHost(String host,boolean stopNow) throws MetaMatrixComponentException,
                                         MultipleException {
        
        stoppedHosts.add(host);
    }

    public void stopProcess(VMControllerID processID,
                            boolean stopNow) throws AuthorizationException,
                                            MetaMatrixComponentException {
        stoppedProcesses.add(processID.toString());
    }

    public void stopService(ServiceID serviceID,
                            boolean stopNow) throws MetaMatrixComponentException {
        stoppedServices.add(serviceID.toString());        
    }

    public synchronized void synchronizeServer() throws MetaMatrixComponentException,
                                                                            MultipleException {
        synchronizeSystem = true;
    }

    
        
    
}