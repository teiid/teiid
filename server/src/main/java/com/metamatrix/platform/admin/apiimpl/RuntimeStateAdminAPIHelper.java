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

package com.metamatrix.platform.admin.apiimpl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.AdminException;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.MetaMatrixExceptionUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.admin.api.runtime.SystemStateBuilder;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.HostControllerRegistryBinding;
import com.metamatrix.platform.registry.ProcessRegistryBinding;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.LogPlatformConstants;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;
import com.metamatrix.platform.vm.controller.ProcessStatistics;
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
    public SystemState getSystemState() throws MetaMatrixComponentException {
        try {
            SystemStateBuilder ssm = new SystemStateBuilder(this.registry, this.hostManagement);
            return ssm.getSystemState();
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.ADMIN_0051, PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0051));
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
    public ProcessStatistics getVMStatistics(String hostName, String processName) throws MetaMatrixComponentException {
		ProcessRegistryBinding vm = registry.getProcessBinding(hostName, processName);
		return vm.getProcessController().getVMStatistics();
    }   
    

    public InetAddress getVMHostName(String hostName, String processName) throws MetaMatrixComponentException  {
		ProcessRegistryBinding vm = registry.getProcessBinding(hostName, processName);
		return vm.getProcessController().getAddress();    	
    }
    
    /**
     * Return Collection of QueueStats for service.
     * 
     * @param binding
     *            The {@link ServiceRegistryBinding} for the connector 
     * @return Collection of QueueStats objects.
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
     * Return Collection of ConnectionPoolStats for a service.
     * 
     * @param binding
     *            The {@link ServiceRegistryBinding} for the connector 
     * @return Collection of ConnectionPoolStat objects.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public Collection getConnectionPoolStats(ServiceRegistryBinding binding) throws MetaMatrixComponentException {
		ServiceInterface service = binding.getService();
		if (service != null) {
		    return service.getConnectionPoolStats();
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
        return this.registry.getServiceBinding(serviceID.getHostName(), serviceID.getProcessName(), serviceID);
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
        	if (binding.getServiceType().equals(serviceType) && (currentState == ServiceState.STATE_OPEN || currentState == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE)) {
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
    public void shutdownServer() throws MetaMatrixComponentException {
    	this.hostManagement.killAllServersInCluster();
    }
    
    
    /**
     * Shutdown server and restart.
     * 
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public void bounceServer() throws MetaMatrixComponentException {
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

    public void synchronizeServer() throws MetaMatrixComponentException,MultipleException {
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
                } catch (MetaMatrixComponentException e) {
                    exceptions.add(e);
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
                ProcessManagement vmController = null;
                // if registered then get the VMController
                if (pData.isRegistered()) {
                    try {
                    	ProcessRegistryBinding vmBinding = this.registry.getProcessBinding(pData.getHostName(), pData.getName());
                        vmController = vmBinding.getProcessController();
                    } catch (ResourceNotBoundException e) {
                    	exceptions.add(new MetaMatrixComponentException(e, e.getMessage()));
                        break;
                    }
                }
                // deployed/notRunning - start process via MetaMatrixController
                if (pData.isDeployed() && !pData.isRegistered()) {
                    try {
                    	this.hostManagement.startServer(hostName, pData.getName());
                    } catch (MetaMatrixComponentException e) {
                        exceptions.add(e);
                    }
                    // not deployed/Running - stopVM and then kill via MetaMatrixController
                } else if (!pData.isDeployed() && pData.isRegistered()) {
                    this.hostManagement.killServer(hostName, pData.getName(), true);
                    // deployed/running - get list of services for this process
                    // loop thru and insure all deployed services are running
                    // kill all non-deployed running services.
                } else {
                        Collection services = pData.getServices();
                        Iterator sIter = services.iterator();
                        // looping thru services
                        while (sIter.hasNext()) {
	                        try {
	                            ServiceData sData = (ServiceData)sIter.next();
	                            // if not deployed but running then kill, kill, kill
	                            if (!sData.isDeployed() && sData.isRegistered()) {
	                                vmController.stopService(sData.getServiceID(), false, true);
	                                // if deployed but not running then start
	                            } else if (sData.isDeployed() && !sData.isRegistered()) {
	                                vmController.startDeployedService((ServiceComponentDefnID)sData.getComponentDefnID());
	                                // deployed and registered
	                                // make sure we are running
	                            } else if (sData.isDeployed() && sData.isRegistered()) {
	                                ServiceID serviceID = sData.getServiceID();
	                                switch (sData.getCurrentState()) {
	                                    case ServiceState.STATE_CLOSED:
	                                    case ServiceState.STATE_FAILED:
	                                    case ServiceState.STATE_INIT_FAILED:
	                                        vmController.startService(serviceID);
	                                        break;
	
	                                    case ServiceState.STATE_DATA_SOURCE_UNAVAILABLE:
	                                        vmController.checkService(serviceID);
	                                        break;
	
	                                    default:
	                                }
	                            }
	                        } catch (ServiceException e) {
	                        	exceptions.add(new MetaMatrixComponentException(e, e.getMessage()));
	                        }
                        }
                    
                }
            }
        }
        if (!exceptions.isEmpty()) {
            throw new MultipleException(exceptions, ErrorMessageKeys.ADMIN_0054,PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0054, errorMsg));
        }
    }

    public ProcessManagement getVMControllerInterface(String hostName, String processName) throws ResourceNotBoundException {
    	return this.registry.getProcessBinding(hostName, processName).getProcessController();
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

        ProcessRegistryBinding vmBinding = this.registry.getProcessBinding(serviceID.getHostName(), serviceID.getProcessName());
        
        ProcessManagement vmController = vmBinding.getProcessController();
        try {
        	vmController.stopService(serviceID, false, false);
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
     * Set the Log Configuration in the database and propagate changes to other VM
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
                                           String principalName) throws 
                                                                ConfigurationException,
                                                                ServiceException,
                                                                MetaMatrixComponentException {

        // Config svc proxy
        ConfigurationServiceInterface configAdmin = PlatformProxyHelper.getConfigurationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        // First set the log config in the database
        configAdmin.executeTransaction(actions, principalName);

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
                ProcessRegistryBinding vmBinding = (ProcessRegistryBinding)vmItr.next();
                ProcessManagement vm = vmBinding.getProcessController();
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

        ProcessRegistryBinding binding = registry.getProcessBinding(serviceID.getHostName(), serviceID.getProcessName());
        ProcessManagement vmController = binding.getProcessController();
        try {
        	vmController.stopService(serviceID, stopNow, false);
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
   public void stopProcess(String hostName, String processName, boolean stopNow)
			throws AuthorizationException, MetaMatrixComponentException {
		this.hostManagement.killServer(hostName, processName, stopNow);
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
   public byte[] exportLogs(String hostName, String processName) throws MetaMatrixComponentException {
	   ProcessRegistryBinding processBinding = this.registry.getProcessBinding(hostName, processName);
       ProcessManagement vmController = processBinding.getProcessController();
       return vmController.exportLogs();
   }
   
   /**
    * Return all processes.
    * 
    * @return List of processes
    * @throws MetaMatrixComponentException
    *             if an error occurred in communicating with a component.
    */
   public List<ProcessRegistryBinding> getProcesses() throws MetaMatrixComponentException {
       return registry.getVMs(null);
   }
   
   public Date getEldestProcessStartTime() throws MetaMatrixComponentException {
	   long start = 0;
	   for (ProcessRegistryBinding processRegistryBinding : getProcesses()) {
		   if (processRegistryBinding.isAlive()) {
			   start = Math.max(start, processRegistryBinding.getStartTime());
		   }
	   }
	   if (start != 0) {
		   return new Date(start); 
	   }
	   return null;
   }

}
