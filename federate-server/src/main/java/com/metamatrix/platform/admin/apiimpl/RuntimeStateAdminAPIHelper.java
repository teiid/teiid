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

package com.metamatrix.platform.admin.apiimpl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.common.actions.ModificationException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.api.exceptions.ConfigurationLockException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.pooling.api.ResourcePoolMgr;
import com.metamatrix.common.pooling.api.ResourcePoolStatistics;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.core.util.MetaMatrixExceptionUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.PSCData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ResourcePoolStats;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.admin.api.runtime.SystemStateBuilder;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.HostControllerRegistryBinding;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ResourcePoolMgrBinding;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.registry.VMRegistryBinding;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.LogPlatformConstants;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.vm.api.controller.VMControllerInterface;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.platform.vm.controller.VMStatistics;
import com.metamatrix.server.HostManagement;


/**
 * Singleton helper class for performing Runtime administrative functionality. 
 * @since 4.3
 */
public class RuntimeStateAdminAPIHelper {
    
    //singleton instance
    private static RuntimeStateAdminAPIHelper instance;

    private ClusteredRegistryState registry;
    HostManagement hostManagement;
    
    
    protected RuntimeStateAdminAPIHelper(ClusteredRegistryState registry, HostManagement hostManagement) {
    	this.registry = registry;
    	this.hostManagement = hostManagement;
    }
    
    /**
     * Get the singleton instance. 
     * @return
     * @since 4.3
     */
    public static synchronized RuntimeStateAdminAPIHelper getInstance(ClusteredRegistryState registry, HostManagement hostManagement) {
        if (instance == null) {
            instance = new RuntimeStateAdminAPIHelper(registry, hostManagement);       
        }
        
        return instance;
    }
    
    
    /**
     * Return the running state of the system.
     * 
     * @return SysteState object that represents the system.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized SystemState getSystemState() throws MetaMatrixComponentException {
        try {
            SystemStateBuilder ssm = new SystemStateBuilder(this.registry, this.hostManagement);
            return ssm.getSystemState();
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.ADMIN_0051,
                                                   PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0051));
        }
    }

    /**
     * Return TRUE if the system is started; i.e. at leat one of every essential services in a product is running. Authorization,
     * Configuration, Membership and Session services are considered to be essential.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @return Boolean - TRUE if system is started, FALSE if not.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public boolean isSystemStarted() throws MetaMatrixComponentException {
        boolean systemStarted = false;
        List authServices = registry.getServiceBindings(AuthorizationServiceInterface.NAME);
        List sessionServices = registry.getServiceBindings(SessionServiceInterface.NAME);
        List membershipServices = registry.getServiceBindings(MembershipServiceInterface.NAME);
        List configurationServices = registry.getServiceBindings(ConfigurationServiceInterface.NAME);
        if ((authServices.size() > 0)
            && (sessionServices.size()) > 0
            && (membershipServices.size() > 0)
            && (configurationServices.size() > 0)) {

            systemStarted = true;
        }
        return systemStarted;
    } 
    /**
     * Return all hosts running in mm system.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @return List of Strings
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public List<String> getHosts() throws MetaMatrixComponentException {
    	List<String> list = new ArrayList<String>();
    	List<HostControllerRegistryBinding> allHosts = this.registry.getHosts();
    	for(HostControllerRegistryBinding host:allHosts) {
    		list.add(host.getHostName());
    	}
        return list;
    }

    /**
     * Return all processes running in mm system.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @return List of VMControllerIDs.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public List<VMControllerID> getProcesses() throws MetaMatrixComponentException {
        List processes = new ArrayList();

        List<VMRegistryBinding> bindings = this.registry.getVMs(null);
        for (VMRegistryBinding vmBinding:bindings) {
            processes.add(vmBinding.getVMControllerID());
        }
        return processes;
    }

    /**
     * Return VMStatistics object for Process.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @param VMControllerID
     *            ID of the process.
     * @return VMStatistics.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public VMStatistics getVMStatistics(VMControllerID vmID) throws MetaMatrixComponentException {
		VMRegistryBinding vm = registry.getVM(vmID.getHostName(), vmID.toString());
		return vm.getVMController().getVMStatistics();
    }   
    

    public InetAddress getVMHostName(VMControllerID vmID) throws MetaMatrixComponentException  {
		VMRegistryBinding vm = registry.getVM(vmID.getHostName(), vmID.toString());
		return vm.getVMController().getAddress();    	
    }
    
    /**
     * Return Collection of QueueStats for service.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @param serviceID
     *            ID of the service.
     * @return Collection of QueueStats objects.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public Collection getServiceQueueStatistics(ServiceRegistryBinding binding) throws MetaMatrixComponentException {
		ServiceInterface service = binding.getService();
		if (service != null) {
		    return service.getQueueStatistics();
		}
        return Collections.EMPTY_LIST;
    }      

    /**
     * Return ServiceRegistryBinding for the given serviceID
     * 
     * @param serviceID
     *            Identifies service
     * @return ServiceRegistryBinding
     */
    public ServiceRegistryBinding getServiceBinding(ServiceID serviceID) throws ResourceNotBoundException {
        return this.registry.getServiceBinding(serviceID.getHostName(), serviceID.getVMControllerID().toString(), serviceID);
    }

    /**
     * Return serviceID's for all services running in mm system.
     * 
     * @param callerSessionID
     *            ID of the caller's current session.
     * @return List of ServiceIDs.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public List<ServiceRegistryBinding> getServices() throws MetaMatrixComponentException {
        return registry.getServiceBindings(null, null);
    }

    /**
     * get the active services of given type
     * @param serviceType
     * @return
     * @throws MetaMatrixComponentException
     */
    public List<ServiceRegistryBinding> getActiveServices(String serviceType) throws MetaMatrixComponentException {
        ArrayList list = new ArrayList();
    	List<ServiceRegistryBinding> bindings = registry.getServiceBindings(null, null);
        
        for(ServiceRegistryBinding binding:bindings) {
        	int currentState = binding.getCurrentState();
        	if (binding.getServiceType().equals(serviceType) && (currentState == ServiceInterface.STATE_OPEN || currentState == ServiceInterface.STATE_DATA_SOURCE_UNAVAILABLE)) {
        		list.add(binding);
        	}
        }
        return list;
    }
    

    /**
     * Gracefully shutdown server waiting for work to complete.
     * 
     * @param registry
     *            used by MetaMatrix to find the active VMs
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void shutdownServer() throws MetaMatrixComponentException {
    	this.hostManagement.killAllServersInCluster();
    }
    
    
    /**
     * Shutdown server and restart.
     * 
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void bounceServer() throws MetaMatrixComponentException {
    	this.hostManagement.bounceAllServersInCluster();
    }
    
    /**
     * Synchronize running services with runtime configuration. Any running hosts/processes/services that are running but not in
     * the configuration are killed Any deployed hosts/processes/services that are not running but are deployed are started Any
     * deployed services that are running are set to the open state.
     * 
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     * @throws a MultipleException if an error occurs
     */

    public synchronized void synchronizeServer() throws MetaMatrixComponentException,MultipleException {
        List exceptions = new ArrayList();
        StringBuffer errorMsg = new StringBuffer();

        SystemState state = getSystemState();

        // ---------------------------------------------------------------------
        // get the list of hosts
        // loop thru hosts and start any host that isn't running
        // then remove host from Collection of hosts to be processed.
        // ---------------------------------------------------------------------
        Collection hosts = state.getHosts();
        List newHosts = new ArrayList();
        Iterator hostIter = hosts.iterator();
        while (hostIter.hasNext()) {
            HostData hData = (HostData)hostIter.next();

            if (hData.isDeployed() && !hData.isRegistered()) {
                try {
                	this.hostManagement.startServers(hData.getName());
                } catch (Exception e) {
                    exceptions.add(e);
                    // errorMsg.append(e.getMessage());
                }
                newHosts.add(hData);
            }
        }
        // remove hosts that have just been started from the list.
        hosts.removeAll(newHosts);

        // ---------------------------------------------------------------------
        // Get a map of HostData -> Collection of processes
        // ---------------------------------------------------------------------
        hostIter = hosts.iterator();
        Map processes = new HashMap();
        while (hostIter.hasNext()) {
            HostData hData = (HostData)hostIter.next();
            processes.put(hData.getName(), hData.getProcesses());
        }

        // ---------------------------------------------------------------------
        // Iterate thru each list for each hostData object
        // If the process is deployed but not running then start process
        // Else if the process is running but not deployed then stop process
        // Else get all the services for each process/host and insure all deployed
        // services are running.
        // ---------------------------------------------------------------------
        Iterator procKeys = processes.keySet().iterator();
        while (procKeys.hasNext()) {
            String hostName = (String)procKeys.next();

            List procList = (List)processes.get(hostName);
            Iterator procIter = procList.iterator();

            while (procIter.hasNext()) {
                ProcessData pData = (ProcessData)procIter.next();
                VMControllerInterface vmController = null;
                // if registered then get the VMController
                if (pData.isRegistered()) {
                    try {
                    	VMRegistryBinding vmBinding = this.registry.getVM(pData.getProcessID().getHostName(), pData.getProcessID().toString());
                        vmController = vmBinding.getVMController();
                    } catch (Exception e) {
                        exceptions.add(e); // if we can't get the vmController then go to next process
                        // errorMsg.append(e.getMessage());
                        break;
                    }
                }
                // deployed/notRunning - start process via MetaMatrixController
                if (pData.isDeployed() && !pData.isRegistered()) {
                    try {
                    	this.hostManagement.startServer(hostName, pData.getName());
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                    // not deployed/Running - stopVM and then kill via MetaMatrixController
                } else if (!pData.isDeployed() && pData.isRegistered()) {
                    this.hostManagement.killServer(hostName, pData.getName(), true);
                    // deployed/running - get list of services for this process
                    // loop thru and insure all deployed services are running
                    // kill all non-deployed running services.
                } else {
                    Collection pscList = pData.getPSCs();
                    Iterator pscIter = pscList.iterator();
                    while (pscIter.hasNext()) {
                        PSCData pscData = (PSCData)pscIter.next();
                        Collection services = pscData.getServices();
                        Iterator sIter = services.iterator();
                        // looping thru services
                        while (sIter.hasNext()) {
                            ServiceData sData = (ServiceData)sIter.next();
                            // if not deployed but running then kill, kill, kill
                            if (!sData.isDeployed() && sData.isRegistered()) {
                                try {
                                    vmController.shutdownService(sData.getServiceID());
                                } catch (Exception e) {
                                    exceptions.add(e);
                                }
                                // if deployed but not running then start
                            } else if (sData.isDeployed() && !sData.isRegistered()) {
                                try {
                                    vmController.startDeployedService((ServiceComponentDefnID)sData.getComponentDefnID());
                                } catch (Exception e) {
                                    exceptions.add(e);
                                }
                                // deployed and registered
                                // make sure we are running
                            } else if (sData.isDeployed() && sData.isRegistered()) {
                                ServiceID serviceID = sData.getServiceID();
                                try {
                                    switch (sData.getCurrentState()) {
                                        case ServiceInterface.STATE_CLOSED:
                                        case ServiceInterface.STATE_FAILED:
                                        case ServiceInterface.STATE_INIT_FAILED:
                                            vmController.startService(serviceID);
                                            break;

                                        case ServiceInterface.STATE_DATA_SOURCE_UNAVAILABLE:
                                            vmController.checkService(serviceID);
                                            break;

                                        default:
                                    }
                                } catch (Exception e) {
                                    exceptions.add(e);
                                }
                            }
                        }
                    }
                }
            }
        }
        if (!exceptions.isEmpty()) {
            throw new MultipleException(exceptions, ErrorMessageKeys.ADMIN_0054,PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0054, errorMsg));
        }
    }

    
    /**
     * Returns a Collection of {@link com.metamatrix.common.config.api.ResourceDescriptor ResourceDescriptor}for all resource
     * pools defined to the system.
     * 
     * @param registry
     */
    public Collection getResourceDescriptors()throws ResourcePoolException, MetaMatrixComponentException {

        Set stats = new HashSet();
        Collection result = new ArrayList();

        Iterator poolIter = this.registry.getResourcePoolManagerBindings(null, null).iterator();
        while (poolIter.hasNext()) {
            ResourcePoolMgrBinding binding = (ResourcePoolMgrBinding)poolIter.next();
            ResourcePoolMgr mgr = binding.getResourcePoolMgr();

            // find the first resource descriptor for this id,
            // all descriptors across all the vms are not being
            // maintained independently

            Collection rds = mgr.getAllResourceDescriptors();
            for (Iterator it = rds.iterator(); it.hasNext();) {
                ResourceDescriptor descriptor = (ResourceDescriptor)it.next();
                if (!stats.contains(descriptor.getID())) {
                    stats.add(descriptor.getID());
                    result.add(descriptor);
                }
            }

        }
        return result;

    }

    /**
     * Returns a Collection of {@link com.metamatrix.platform.admin.api.runtime.ResourcePoolStats ResourcePoolStats} for all
     * resource pools known to the system.
     */
    public Collection getResourcePoolStatistics() throws MetaMatrixComponentException, ResourcePoolException {

        Collection result = new ArrayList();

        Iterator poolIter = this.registry.getResourcePoolManagerBindings(null, null).iterator();
        while (poolIter.hasNext()) {
            ResourcePoolMgrBinding binding = (ResourcePoolMgrBinding)poolIter.next();
            ResourcePoolMgr mgr = binding.getResourcePoolMgr();

            Iterator iter = mgr.getResourcePoolStatistics().iterator();
            while (iter.hasNext()) {
                ResourcePoolStatistics stats = (ResourcePoolStatistics)iter.next();
                Collection resStats = mgr.getResourcesStatisticsForPool(stats.getResourceDescriptorID());
                String processName = getVMControllerInterface(binding.getID().getVMControllerID()).getName();
                ResourcePoolStats poolStats = new ResourcePoolStats(stats, stats.getResourceDescriptorID(),
                                                                        binding.getID().getHostName(),
                                                                        processName, resStats);

                result.add(poolStats);
            }
        }
        return result;
    }

    public VMControllerInterface getVMControllerInterface(VMControllerID id) throws ResourceNotBoundException {
    	return this.registry.getVM(id.getHostName(), id.toString()).getVMController();
    } 
    
    /**
     * Start a deployed service.
     * 
     * @param registry
     *            The Registry.
     * @param id
     *            ServiceComponentDefnID of service instance.
     * @param vmID
     *            Identifies VMController to start service in.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public void startDeployedService(ServiceComponentDefnID serviceID, VMControllerID vmID) throws MetaMatrixComponentException {

        VMControllerInterface vmController = getVMControllerInterface(vmID);
        try {
            vmController.startDeployedService(serviceID);
        } catch (ServiceException se) {
            throw new MetaMatrixComponentException(se);
        }
    }

    /**
     * Restart a failed or stopped service.
     * 
     * @param registry
     *            the Registry.
     * @param serviceID
     *            ID of service instance.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public void restartService(ServiceID serviceID) throws MetaMatrixComponentException {

        VMControllerID vmID = serviceID.getVMControllerID();
        VMRegistryBinding vmBinding = this.registry.getVM(vmID.getHostName(), vmID.toString());
        
        VMControllerInterface vmController = vmBinding.getVMController();
        try {
        	vmController.stopService(serviceID);
        } catch (ServiceException se) {
        	LogManager.logDetail(LogPlatformConstants.CTX_RUNTIME_ADMIN, se, "Service exception stopping service during restart"); //$NON-NLS-1$
        }
        try {
            vmController.startService(serviceID);
        } catch (ServiceException se) {
            throw new MetaMatrixComponentException(se, MetaMatrixExceptionUtil.getLinkedMessages(se));
        }
    } 

    /**
     * Set the Log Configuration in the database and propergate changes to other VM
     * 
     * @param registry
     * @param config
     * @param logConfig
     * @param actions
     * @param token
     * @throws ConfigurationLockException
     * @throws ConfigurationException
     * @throws ServiceException
     * @throws MetaMatrixComponentException
     * @throws RegistryCommunicationException
     * @since 4.3
     */
    public void setLogConfiguration(Configuration config,
                                           LogConfiguration logConfig,
                                           List actions,
                                           String principalName) throws ConfigurationLockException,
                                                                ConfigurationException,
                                                                ServiceException,
                                                                MetaMatrixComponentException {

        // Config svc proxy
        ConfigurationServiceInterface configAdmin = PlatformProxyHelper.getConfigurationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        // First set the log config in the database
        try {
            configAdmin.executeTransaction(actions, principalName);
        } catch (ModificationException e) {
            String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0084, config.getID());
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.ADMIN_0084, msg);
        }

        // Then, if the operational (current) config is effected, set logging config for
        // LogManager in each VM.
        StringBuffer msgs = null;
        ConfigurationID currentConfigID = (ConfigurationID)config.getID();
        ConfigurationID operationalConfigID = configAdmin.getCurrentConfigurationID();

        if (currentConfigID.equals(operationalConfigID)) {
            LogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, PlatformPlugin.Util.getString(LogMessageKeys.ADMIN_0028));

            // Set in this (AppServer) VM since registry won't have a handle to it...
            LogManager.setLogConfiguration(logConfig);

            Iterator vmItr = registry.getVMs(null).iterator();
            while (vmItr.hasNext()) {
                VMRegistryBinding vmBinding = (VMRegistryBinding)vmItr.next();
                VMControllerInterface vm = vmBinding.getVMController();
                    vm.setCurrentLogConfiguration(logConfig);
            }
            if (msgs != null && msgs.length() > 0) {
                throw new MetaMatrixComponentException(ErrorMessageKeys.ADMIN_0086,PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0086, msgs.toString()));
            }
        }
    }

    /**
     * Stop service.
     * 
     * @param registry
     *            the Registry.
     * @param serviceID
     *            ID of service instance.
     * @param stopNow
     *            If true, stop forcefully. If false, wait until work is complete.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public void stopService(ServiceID serviceID, boolean stopNow) throws MetaMatrixComponentException {

        VMControllerID vmID = serviceID.getVMControllerID();
        VMRegistryBinding binding = registry.getVM(vmID.getHostName(), vmID.toString());
        VMControllerInterface vmController = binding.getVMController();
        try {
            if (stopNow) {
                vmController.stopServiceNow(serviceID);
            } else {
                vmController.stopService(serviceID);
            }
        } catch (ServiceException se) {
            throw new MetaMatrixComponentException(se);
        }
    }
    
    /**
     * Start Host and all processes/services for host.
     * 
     * @param registry
     *            The Registry.
     * @param host
     *            Name of host to start.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
   public  void startHost(String host) throws MetaMatrixComponentException {
	   this.hostManagement.startServers(host);
    }    

    /**
     * Start Process and all services for process.
     * 
     * @param registry
     *            The Registry.
     * @param host
     *            Host processes belongs to.
     * @param process
     *            Name of process to start.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
   public void startProcess(String host, String process) throws MetaMatrixComponentException {

       boolean hostFound = false;
       boolean processFound = false;
       SystemState state = getSystemState();

       Collection hosts = state.getHosts();
       Iterator hostIter = hosts.iterator();
       while (hostIter.hasNext()) {
           HostData hData = (HostData)hostIter.next();
           if (hData.getName().equalsIgnoreCase(host)) {
               hostFound = true;

               // host is not running, it must be to start the process
               if (!hData.isRegistered()) {
                   throw new MetaMatrixComponentException(ErrorMessageKeys.ADMIN_0056,
                                                          PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0056, host));
               }

               Collection processes = hData.getProcesses();
               Iterator procIter = processes.iterator();
               while (procIter.hasNext()) {
                   ProcessData pData = (ProcessData)procIter.next();

                   if (pData.getName().equalsIgnoreCase(process)) {
                       processFound = true;
                       // process is already running, cannot start
                       if (pData.isRegistered()) {
                           throw new MetaMatrixComponentException(ErrorMessageKeys.ADMIN_0067,PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0067,process));
                       }
                       
                       this.hostManagement.startServer(host, process);
                   }
               }

           }
       }

       if (!hostFound) {
           throw new MetaMatrixComponentException(PlatformPlugin.Util.getString("RuntimeStateAdminAPIImple.Component_not_found", new Object[] {"Host", host})); //$NON-NLS-1$ //$NON-NLS-2$
       }
       if (!processFound) {
           throw new MetaMatrixComponentException(PlatformPlugin.Util.getString("RuntimeStateAdminAPIImple.Component_not_found", new Object[] {"Process", process})); //$NON-NLS-1$ //$NON-NLS-2$
       }
   } 

    /**
     * Stop host processes/services.
     * 
     * @param registry
     *            The Registry.
     * @param host
     *            Name of host.
     * @param stopNow
     *            If true, stop forcefully. If false, wait until work is complete.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
   public void stopHost(String host, boolean stopNow) throws MetaMatrixComponentException, MultipleException {
	   this.hostManagement.killServers(host, stopNow);
   }

    /**
     * Stop process.
     * 
     * @param registry
     *            The Registry.
     * @param processID
     *            <code>VMControllerID</code>.
     * @param stopNow
     *            If true, stop forcefully. If false, wait until work is complete.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
   public void stopProcess(VMControllerID vmID, boolean stopNow)
			throws AuthorizationException, MetaMatrixComponentException {
		this.hostManagement.killServer(vmID.getHostName(), vmID.toString(), stopNow);
	} 
    
    
    /**
     * Export the server logs to a byte[].  The bytes contain the contents of a .zip file containing the logs. 
     * @param registry
     * @param processID  Identifies the process to use to get the logs.  This will export all logs on the host that
     * contains the speciefied process.
     * @return the logs, as a byte[].
     * @throws AdminException
     * @since 4.3
     */
   public byte[] exportLogs(VMControllerID processID) throws MetaMatrixComponentException {
       	VMRegistryBinding vmBinding = this.registry.getVM(processID.getHostName(), processID.toString());
       VMControllerInterface vmController = vmBinding.getVMController();
       return vmController.exportLogs();
   }

}
