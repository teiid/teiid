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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.log.reader.DBLogReader;
import com.metamatrix.common.log.reader.LogReader;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.PSCData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.PscID;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.registry.VMRegistryBinding;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.LogPlatformConstants;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.vm.api.controller.VMControllerInterface;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.platform.vm.controller.VMStatistics;
import com.metamatrix.server.HostManagement;

public class RuntimeStateAdminAPIImpl extends SubSystemAdminAPIImpl implements RuntimeStateAdminAPI {

    // Config svc proxy
    private ConfigurationServiceInterface configAdmin;

    protected Set listeners = new HashSet();

    private RuntimeStateAdminAPIHelper helper;
    
    private LogReader logReader;
    
    private ClusteredRegistryState registry;

    //singleton instance
    private static RuntimeStateAdminAPIImpl runtimeStateAdminAPI;
    
    
    /**
     * ctor
     */
    private RuntimeStateAdminAPIImpl(ClusteredRegistryState registry, HostManagement hostManagement) throws MetaMatrixComponentException {
    	this.registry = registry;
        configAdmin = PlatformProxyHelper.getConfigurationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);       
        helper = RuntimeStateAdminAPIHelper.getInstance(registry, hostManagement);
    }

    public synchronized static RuntimeStateAdminAPIImpl getInstance(ClusteredRegistryState registry, HostManagement hostManagement) throws MetaMatrixComponentException {
        if (runtimeStateAdminAPI == null) {
            runtimeStateAdminAPI = new RuntimeStateAdminAPIImpl(registry, hostManagement);
        }
        return runtimeStateAdminAPI;
    }

    /**
     * Return TRUE if the system is started; i.e. at leat one of every essential services in a product is running. Authorization,
     * Configuration, Membership and Session services are considered to be essential.
     * 
     * @return Boolean - TRUE if system is started, FALSE if not.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized boolean isSystemStarted() throws AuthorizationException,
                                                                                    InvalidSessionException,
                                                                                    MetaMatrixComponentException {

        // Validate caller's session
        //        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        return helper.isSystemStarted();
    }

    /**
     * Return serviceID's for all services running in mm system.
     * 
     * @return List of ServiceIDs.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized List<ServiceRegistryBinding> getServices() throws AuthorizationException,
                                                                             InvalidSessionException,
                                                                             MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        return helper.getServices();

    }

    /**
     * Return all processes running in mm system.
     * 
     * @return List of VMControllerIDs.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized List<VMControllerID> getProcesses() throws AuthorizationException,
                                                                              InvalidSessionException,
                                                                              MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        
        return helper.getProcesses();
    }

    /**
     * Return all hosts running in mm system.
     * 
     * @return List of HostRegistryBindings.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized List<String> getHosts() throws AuthorizationException,
                                                                          InvalidSessionException,
                                                                          MetaMatrixComponentException {

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        
        return helper.getHosts();
    }

    /**
     * Stop service once work is complete.
     * 
     * @param serviceID
     *            ID of service instance.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void stopService(ServiceID serviceID) throws AuthorizationException,
                                                             InvalidSessionException,
                                                             MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0003, new Object[] {
            serviceID, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.stopService(" + serviceID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        helper.stopService(serviceID, false);
    }

    /**
     * Stop service now.
     * 
     * @param serviceID
     *            ID of service instance.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void stopServiceNow(ServiceID serviceID) throws AuthorizationException,
                                                                InvalidSessionException,
                                                                MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0004, new Object[] {
            serviceID, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.stopServiceNow(" + serviceID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        helper.stopService(serviceID, true);
    }

    /**
     * Stop host processes/services once work is complete.
     * 
     * @param host
     *            Name of host.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void stopHost(String host) throws AuthorizationException,
                                                  InvalidSessionException,
                                                  MetaMatrixComponentException,
                                                  MultipleException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0005, new Object[] {
            host, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.stopHost(" + host + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        helper.stopHost(host, false);
    }

    /**
     * Stop host processes/services now.
     * 
     * @param host
     *            Name of host.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void stopHostNow(String host) throws AuthorizationException,
                                                     InvalidSessionException,
                                                     MetaMatrixComponentException,
                                                     MultipleException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0006, new Object[] {
            host, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.stopHostNow(" + host + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        helper.stopHost(host, true);
    }
  

    /**
     * Stop process once work is complete.
     * 
     * @param processID
     *            <code>VMControllerID</code>.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void stopProcess(VMControllerID processID) throws AuthorizationException,
                                                                  InvalidSessionException,
                                                                  MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0009, new Object[] {
            processID, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.stopProcess(" + processID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        helper.stopProcess(processID, false);
    }

    /**
     * Stop process now.
     * 
     * @param processID
     *            <code>VMControllerID</code>.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void stopProcessNow(VMControllerID processID) throws AuthorizationException,
                                                                     InvalidSessionException,
                                                                     MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0010, new Object[] {
            processID, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.stopProcessNow(" + processID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        helper.stopProcess(processID, true);
    }

  
    /**
     * Gracefully shutdown server waiting for work to complete.
     * 
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void shutdownServer() throws AuthorizationException,
                                                                                InvalidSessionException,
                                                                                MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0015, new Object[] {
            token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.shutdownServer()"); //$NON-NLS-1$

        helper.shutdownServer();

    }

    /**
     * Shutdown server and restart.
     * 
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void bounceServer() throws AuthorizationException,
                                                                              InvalidSessionException,
                                                                              MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        LogManager.logCritical(LogPlatformConstants.CTX_RUNTIME_ADMIN, CorePlugin.Util.getString(LogMessageKeys.ADMIN_0016, token.getUsername()));

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.bounceServer()"); //$NON-NLS-1$

        
        helper.bounceServer();
    }

 
    /**
     * Restart a failed or stopped service.
     * 
     * @param serviceID
     *            ID of service instance.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void restartService(ServiceID serviceID) throws AuthorizationException,
                                                                InvalidSessionException,
                                                                MetaMatrixComponentException {

        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0019, new Object[] {
            serviceID
        });

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.restartService(" + serviceID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        
        helper.restartService(serviceID);
    }

    /**
     * Start a deployed service.
     * 
     * @param id
     *            ServiceComponentDefnID of service instance.
     * @param vmID
     *            Identifies VMController to start service in.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void startDeployedService(ServiceComponentDefnID serviceID,
                                                  VMControllerID vmID) throws AuthorizationException,
                                                                      InvalidSessionException,
                                                                      MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0019, new Object[] {
            serviceID
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.startDeployedService(" + serviceID + ", " + vmID + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        helper.startDeployedService(serviceID, vmID);
    }

    /**
     * Start Host and all processes/services for host.
     * 
     * @param host
     *            Name of host to start.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void startHost(String host) throws AuthorizationException,
                                                   InvalidSessionException,
                                                   MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0020, new Object[] {
            host, token.getUsername()
        });
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.startHost(" + host + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        helper.startHost(host);
    }

    /**
     * Start Process and all services for process.
     * 
     * @param host
     *            Host processes belongs to.
     * @param process
     *            Name of process to start.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void startProcess(String host,
                                          String process) throws AuthorizationException,
                                                         InvalidSessionException,
                                                         MetaMatrixComponentException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0021, new Object[] {
            process, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.startProcess(" + host + ", " + process + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        helper.startProcess(host, process);
    }

    /**
     * Start up all services in psc.
     * 
     * @param pscID
     *            PSC to start.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void startPSC(PscID pscID) throws AuthorizationException,
                                                  InvalidSessionException,
                                                  MetaMatrixComponentException,
                                                  MultipleException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0022, new Object[] {
            pscID, token.getUsername()
        });
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.startPSC(" + pscID + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        SystemState state = helper.getSystemState();
        Iterator hosts = state.getHosts().iterator();
        while (hosts.hasNext()) {
            HostData hostData = (HostData)hosts.next();
            Iterator processes = hostData.getProcesses().iterator();
            while (processes.hasNext()) {
                ProcessData processData = (ProcessData)processes.next();
                Iterator pscs = processData.getPSCs().iterator();
                while (pscs.hasNext()) {
                    PSCData pscData = (PSCData)pscs.next();
                    if (pscData.getPscID().equals(pscID)) {
                        startPSCServices(processData, pscData);
                        return;
                    }
                }
            }
        }
    }

    // Helper method to start all services that are in a PSC
    private void startPSCServices(ProcessData processData,
                                  PSCData pscData) throws MetaMatrixComponentException,
                                                  MultipleException {

        // check if already running
        //if (pscData.isRegistered()) {
        //    throw new MetaMatrixComponentException("PSC " + pscData.getName() + " is already running.");
        //}

        // check that process is running
        if (!processData.isRegistered()) {
            throw new MetaMatrixComponentException(ErrorMessageKeys.ADMIN_0069,
                                                   PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0069,
                                                                                 pscData.getName(),
                                                                                 processData.getName()));
        }

        VMControllerInterface vm = null;
        try {
            vm = helper.getVMControllerInterface(processData.getProcessID());
        } catch (ResourceNotBoundException e) {
            throw new MetaMatrixComponentException(ErrorMessageKeys.ADMIN_0070,
                                                   PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0070,
                                                                                 pscData.getName(),
                                                                                 processData.getName()));
        }

        List exceptions = new ArrayList();

        // loop thru and start each service in psc.
        Iterator services = pscData.getServices().iterator();
        while (services.hasNext()) {
            ServiceData serviceData = (ServiceData)services.next();
            // If already registered then insure proper state.
            if (serviceData.isRegistered()) {
                ServiceID serviceID = serviceData.getServiceID();
                try {
                    switch (serviceData.getCurrentState()) {
                        case ServiceState.STATE_CLOSED:
                        case ServiceState.STATE_FAILED:
                        case ServiceState.STATE_INIT_FAILED:
                            vm.startService(serviceID);
                            break;

                        case ServiceState.STATE_DATA_SOURCE_UNAVAILABLE:
                            vm.checkService(serviceID);
                            break;

                        default:
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
                // service is not registered so start it.
            } else {
                try {
                    vm.startDeployedService((ServiceComponentDefnID)serviceData.getComponentDefnID());
                } catch (ServiceException se) {
                    exceptions.add(se);
                }
            }
        }

        if (!exceptions.isEmpty()) {
            throw new MultipleException(exceptions, ErrorMessageKeys.ADMIN_0073,
                                        PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0073, pscData.getName()));
        }
    }

    
    /** 
     * @see com.metamatrix.platform.admin.api.RuntimeStateAdminAPI#stopPSC(com.metamatrix.platform.admin.api.runtime.PscID)
     * @since 4.3
     */
    public void stopPSCNow(PscID pscID) throws AuthorizationException,
                                       InvalidSessionException,
                                       MetaMatrixComponentException,
                                       MultipleException {
    }
    
    /** 
     * @see com.metamatrix.platform.admin.api.RuntimeStateAdminAPI#stopPSCNow(com.metamatrix.platform.admin.api.runtime.PscID)
     * @since 4.3
     */
    public synchronized void stopPSC(PscID pscID) throws AuthorizationException,
                                                 InvalidSessionException,
                                                 MetaMatrixComponentException,
                                                 MultipleException {
        stopPSC(pscID, false);
    }

    
    
    private synchronized void stopPSC(PscID pscID,
                                      boolean now) throws AuthorizationException,
                                                 InvalidSessionException,
                                                 MetaMatrixComponentException,
                                                 MultipleException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0023, new Object[] {
            pscID, token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.stopPSC(" + pscID + ", " + now + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        SystemState state = getSystemState();
        Iterator hosts = state.getHosts().iterator();
        while (hosts.hasNext()) {
            HostData hostData = (HostData)hosts.next();
            Iterator processes = hostData.getProcesses().iterator();
            while (processes.hasNext()) {
                ProcessData processData = (ProcessData)processes.next();
                Iterator pscs = processData.getPSCs().iterator();
                while (pscs.hasNext()) {
                    PSCData pscData = (PSCData)pscs.next();
                    if (pscData.getPscID().equals(pscID)) {
                        stopPSCServices(processData, pscData, now);
                        return;
                    }
                }
            }
        }
    }

    // Helper method to stop all services that are in a psc
    private void stopPSCServices(ProcessData processData,
                                 PSCData pscData,
                                 boolean now) throws MetaMatrixComponentException,
                                             MultipleException {

        // check if already running
        if (!pscData.isRegistered()) {
            throw new MetaMatrixComponentException(ErrorMessageKeys.ADMIN_0056,
                                                   PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0056, pscData.getName()));
        }

        VMControllerInterface vm = null;
        try {
            vm = helper.getVMControllerInterface(processData.getProcessID());
        } catch (ResourceNotBoundException e) {
            throw new MetaMatrixComponentException(ErrorMessageKeys.ADMIN_0074,
                                                   PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0074,
                                                                                 pscData.getName(),
                                                                                 processData.getName()));
        }

        List exceptions = new ArrayList();

        // loop thru and stop each service in psc.
        Iterator services = pscData.getServices().iterator();
        while (services.hasNext()) {
            ServiceData serviceData = (ServiceData)services.next();
            try {
                ServiceID serviceID = serviceData.getServiceID();
                if (serviceID != null) {
                    vm.stopService(serviceID, now, false);
                }
            } catch (ServiceException se) {
                exceptions.add(se);
            }
        }

        if (!exceptions.isEmpty()) {
            throw new MultipleException(exceptions, ErrorMessageKeys.ADMIN_0076,
                                        PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0076, pscData.getName()));
        }
    }

   
    /**
     * Synchronize running services with runtime configuration. Any running hosts/processes/services that are running but not in
     * the configuration are killed Any deployed hosts/processes/services that are not running but are deployed are started Any
     * deployed services that are running are set to the open state.
     * 
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     * @throws a
     *             MultipleException if an error occurs
     */
    public synchronized void synchronizeServer() throws AuthorizationException,
                                                                                   InvalidSessionException,
                                                                                   MetaMatrixComponentException,
                                                                                   MultipleException {

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        I18nLogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, LogMessageKeys.ADMIN_0026, new Object[] {
            token.getUsername()
        });

        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.synchronizeServer()"); //$NON-NLS-1$

        helper.synchronizeServer();

    }

    /**
     * Returns a Date object representing the time the server was started. If the server is not started a null is returned.
     * 
     * @return Date
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized Date getServerStartTime() throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException {
        try {
            return configAdmin.getServerStartupTime();
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.ADMIN_0083,
                                                   PlatformPlugin.Util.getString(ErrorMessageKeys.ADMIN_0083));
        }
    }

    /**
     * Sets the <code>LogConfiguration</code> on the given <code>Configuration</code>. If the configuration is
     * <code>operational</code>, then the log configuration is set on the <code>Logmanager</code> running in each VM.
     * 
     * @param config
     *            The configuration for which to set the log configuration.
     * @param logConfig
     *            The log configuration with which to affect the log properties.
     * @param actions
     *            The <code>Actions</code> from the <code>ConfigurationObjectEditor</code> used to affect the configuration
     *            database.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized void setLoggingConfiguration(Configuration config,
                                                     LogConfiguration logConfig,
                                                     List actions) throws AuthorizationException,
                                                                  InvalidSessionException,
                                                                  MetaMatrixComponentException {
        LogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, CorePlugin.Util.getString(LogMessageKeys.ADMIN_0027));

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.setLoggingConfiguration(" + config + ", " + logConfig + ", " + actions + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        helper.setLogConfiguration(config, logConfig, actions, token.getUsername());
        
    }



    /**
     * Sets the <code>LogConfiguration</code> on the <code>LogManager</code> running in the given VM. If
     * <code>null>/code> is passed in for vmID, set log config on the
     * App Server VM - the MetaMatrix registry does not have a handle for that VM.
     * @param logConfig The log configuration with which to affect the log properties.
     * @param vmID The ID of the VM for which to set log configuration
     * used to affect the configuration database.  If <code>null</code>, set the
     * App Server VM's log config.
     * @throws AuthorizationException if caller is not authorized to perform this method.
     * @throws InvalidSessionException if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    public synchronized void setLoggingConfiguration(LogConfiguration logConfig,
                                                     VMControllerID vmID) throws AuthorizationException,
                                                                         InvalidSessionException,
                                                                         MetaMatrixComponentException {
        String theVMID = "App Server VM"; //$NON-NLS-1$
        if (vmID != null) {
            theVMID = vmID.toString();
        }
        LogManager.logInfo(LogPlatformConstants.CTX_RUNTIME_ADMIN, CorePlugin.Util.getString(LogMessageKeys.ADMIN_0029, new Object[] {theVMID}));

        // Validate caller's session
        SessionToken token = AdminAPIHelper.validateSession(getSessionID());
        // Validate caller's role
        AdminAPIHelper.checkForRequiredRole(token, AdminRoles.RoleName.ADMIN_PRODUCT, "RuntimeStateAdminAPIImpl.setLoggingConfiguration(" + logConfig + ", " + vmID + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Set App Server VM's log config if vmID == null.
        if (vmID == null) {
            LogManager.setLogConfiguration(logConfig);
            return;
        }

        // Set logging config for given VM
        // VMControllerNotBoundException gets propagated (ancester is MetaMatrixComponentException)
        VMControllerInterface vm = helper.getVMControllerInterface(vmID);
        vm.setCurrentLogConfiguration(logConfig);
    }

    /**
     * Return Collection of QueueStats for service.
     * 
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
    public synchronized Collection getServiceQueueStatistics(ServiceID serviceID) throws AuthorizationException,
                                                                                 InvalidSessionException,
                                                                                 MetaMatrixComponentException {

        LogManager.logDetail(LogPlatformConstants.CTX_RUNTIME_ADMIN, "Getting queue statistics for: " + serviceID); //$NON-NLS-1$
        
        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        return helper.getServiceQueueStatistics(helper.getServiceBinding(serviceID));
    }

    /**
     * Return QueueStats object for queue.
     * 
     * @param serviceID
     *            ID of the service.
     * @param queueName
     *            Name of queue.
     * @return QueueStats object.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized WorkerPoolStats getServiceQueueStatistics(ServiceID serviceID,
                                                                  String queueName) throws AuthorizationException,
                                                                                   InvalidSessionException,
                                                                                   MetaMatrixComponentException {

        LogManager.logDetail(LogPlatformConstants.CTX_RUNTIME_ADMIN,
                             "Getting queue statistics for " + queueName + " for service: " + serviceID); //$NON-NLS-1$ //$NON-NLS-2$

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        ServiceInterface service = helper.getServiceBinding(serviceID).getService();
        return service.getQueueStatistics(queueName);
    }

    /**
     * Return VMStatistics object for Process.
     * 
     * @param VMControllerID
     *            ID of the process.
     * @return VMStatistics.
     * @throws AuthorizationException
     *             if caller is not authorized to perform this method.
     * @throws InvalidSessionException
     *             if the <code>callerSessionID</code> is not valid or is expired.
     * @throws MetaMatrixComponentException
     *             if an error occurred in communicating with a component.
     */
    public synchronized VMStatistics getVMStatistics(VMControllerID vmID) throws AuthorizationException,
                                                                         InvalidSessionException,
                                                                         MetaMatrixComponentException {

        LogManager.logDetail(LogPlatformConstants.CTX_RUNTIME_ADMIN, "Getting vm statistics for " + vmID); //$NON-NLS-1$

        // Validate caller's session
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        
        return helper.getVMStatistics(vmID);
    }

    /**
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPI#getServiceIDByName(java.lang.String,
     *      java.lang.String, java.lang.String)
     * @since 4.2.1
     */
    public ServiceID getServiceIDByName(String hostName,
                                        String processName,
                                        String serviceName) throws AuthorizationException,
                                                           InvalidSessionException,
                                                           MetaMatrixComponentException {
        AdminAPIHelper.validateSession(getSessionID());

        ServiceID result = null;

        Iterator vmIter = this.registry.getVMs(hostName).iterator();
        if (!vmIter.hasNext()) {
            return result;
        }

        while (result == null && vmIter.hasNext()) {
            VMRegistryBinding vmBinding = (VMRegistryBinding)vmIter.next();
            if (vmBinding.getHostName().equalsIgnoreCase(hostName) && vmBinding.getVMName().equalsIgnoreCase(processName)) {

                Iterator serviceIter = this.registry.getServiceBindings(hostName, vmBinding.getVMControllerID().toString()).iterator();
                while (result == null && serviceIter.hasNext()) {
                    ServiceRegistryBinding binding = (ServiceRegistryBinding)serviceIter.next();
                    if (binding.getHostName().equalsIgnoreCase(hostName) &&

                    binding.getInstanceName().trim().equalsIgnoreCase(serviceName)) {
                        result = binding.getServiceID();
                    }
                }
            }
        }
        return result;
    }


    /**
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPI#getPscIDByName(
     *      java.lang.String, java.lang.String, java.lang.String)
     * @since 4.2.1
     */
    public PscID getPscIDByName(String hostName,
                                String processName,
                                String pscName) throws ResourceNotBoundException,
                                               AuthorizationException,
                                               InvalidSessionException,
                                               MetaMatrixComponentException {
        AdminAPIHelper.validateSession(getSessionID());
        PscID result = null;

        Iterator vmIter = registry.getVMs(hostName).iterator();
        if (!vmIter.hasNext()) {
            return result;
        }

        while (result == null && vmIter.hasNext()) {
            VMRegistryBinding vmBinding = (VMRegistryBinding)vmIter.next();
            if (vmBinding.getHostName().equalsIgnoreCase(hostName) && vmBinding.getVMName().equalsIgnoreCase(processName)) {

                Iterator serviceIter = this.registry.getServiceBindings(hostName, vmBinding.getVMControllerID().toString()).iterator();
                while (result == null && serviceIter.hasNext()) {
                    ServiceRegistryBinding binding = (ServiceRegistryBinding)serviceIter.next();
                    if (binding.getHostName().equalsIgnoreCase(hostName) &&

                    binding.getPscID().getName().equalsIgnoreCase(pscName)) {
                        result = new PscID(binding.getPscID(), processName);
                    }
                }
            }
        }
        return result;
    }

    /** 
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPI#getVMControllerBindings()
     * @since 4.2.1
     */
    public List<VMRegistryBinding> getVMControllerBindings() throws InvalidSessionException,
                                                                            AuthorizationException,
                                                                            MetaMatrixComponentException {
        AdminAPIHelper.validateSession(getSessionID());
        return registry.getVMs(null);
    }

    /**
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPI#getVMControllerIDByName(
     *      java.lang.String, java.lang.String)
     * @since 4.2.1
     */
    public VMControllerID getVMControllerIDByName(String hostName,
                                                  String processName) throws AuthorizationException,
                                                                     InvalidSessionException,
                                                                     ResourceNotBoundException,
                                                                     MetaMatrixComponentException {
        AdminAPIHelper.validateSession(getSessionID());
        VMControllerID result = null;
        Iterator vmIter = getVMControllerBindings().iterator();
        if (!vmIter.hasNext()) {
            return result;
        }

        while (vmIter.hasNext()) {
            VMRegistryBinding vmBinding = (VMRegistryBinding)vmIter.next();
            if (vmBinding.getHostName().equalsIgnoreCase(hostName) && vmBinding.getVMName().equalsIgnoreCase(processName)) {
                result = vmBinding.getVMControllerID();
            }
        }
        return result;
    }

    /**
     * @see com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPI#getVMName(
     *      long, java.lang.String)
     * @since 4.2.1
     */
    public String getVMName(long id, String hostName) 
        throws AuthorizationException,InvalidSessionException, MetaMatrixComponentException {
        
        AdminAPIHelper.validateSession(getSessionID());
        VMControllerID vmID = new VMControllerID(id, hostName);
        String result = null;
        Iterator vmIter = getVMControllerBindings().iterator();
        while (vmIter.hasNext()) {
            VMRegistryBinding vmBinding = (VMRegistryBinding)vmIter.next();
            VMControllerID vmIDtemp = vmBinding.getVMController().getID();
            if (vmIDtemp.equals(vmID)) {
                result = vmBinding.getVMName();
                break;
            }

        }
        return result;
    }

    /** 
     * @see com.metamatrix.platform.admin.api.RuntimeStateAdminAPI#getLogEntries(java.util.Date, java.util.Date, java.util.List, java.util.List, int)
     * @since 4.3
     */
    public List getLogEntries(Date startTime,
                              Date endTime,
                              List levels,
                              List contexts,
                              int maxRows) throws AuthorizationException,
                              InvalidSessionException,
                              MetaMatrixComponentException {
        AdminAPIHelper.validateSession(getSessionID());
        
        return getLogReader().getLogEntries(startTime, endTime, levels, contexts, maxRows);
    }

    
    private synchronized LogReader getLogReader() throws MetaMatrixComponentException {
        if (logReader == null) {
            try {
                logReader = new DBLogReader();
            } catch (MetaMatrixException e) {
                throw new MetaMatrixComponentException(e);
            }
        }
        return logReader;
    }

	public SystemState getSystemState() throws AuthorizationException,
			InvalidSessionException, MetaMatrixComponentException {
        AdminAPIHelper.validateSession(getSessionID());
		return helper.getSystemState();
	}
    
}

