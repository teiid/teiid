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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Cache;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.EmbeddedLogger;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.Request;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.server.ServerRuntimeStateAdmin;
import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMProcess;
import com.metamatrix.admin.objects.MMRequest;
import com.metamatrix.admin.objects.MMVDB;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.api.exception.server.InvalidRequestIDException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.core.vdb.VDBStatus;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.RuntimeVDBDeleteUtility;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabaseID;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.service.api.CacheAdmin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;

/**
 * @since 4.3
 */
public class ServerRuntimeStateAdminImpl extends AbstractAdminImpl implements ServerRuntimeStateAdmin {

    
    
    public ServerRuntimeStateAdminImpl(ServerAdminImpl parent, ClusteredRegistryState registry) {
        super(parent, registry);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#cancelRequest(java.lang.String)
     * @since 4.3
     */
    public void cancelRequest(String identifier) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        String sessionID = null; 
        long requestIDLong = -1;
        try {
            String[] identifierParts = null;
			try {
				identifierParts = MMRequest.buildIdentifierArray(identifier);
	            sessionID = identifierParts[0];
	            requestIDLong = Long.parseLong(identifierParts[1]);
			} catch (ArrayIndexOutOfBoundsException e) {
				throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Request_Identifier", new Object[] {identifier, Request.DELIMITER}); //$NON-NLS-1$
			}

        } catch (NumberFormatException e) {
            throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Request_Identifier", new Object[] {identifier, Request.DELIMITER}); //$NON-NLS-1$
        }
            
        try {
            RequestID requestID = new RequestID(sessionID, requestIDLong);
            
            getQueryServiceProxy().cancelQuery(requestID, true);
        } catch(InvalidRequestIDException e) {
        	throw new AdminProcessingException(e);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#cancelSourceRequest(java.lang.String)
     * @since 4.3
     */
    public void cancelSourceRequest(String identifier) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        String sessionID = null; 
        long requestIDLong = -1;
        int nodeID = -1;
        int executionId = -1;
        try {
            String[] identifierParts = MMRequest.buildIdentifierArray(identifier);
            sessionID = identifierParts[0];
            requestIDLong = Long.parseLong(identifierParts[1]);
            nodeID = Integer.parseInt(identifierParts[2]);
            executionId = Integer.parseInt(identifierParts[3]);
        } catch (NumberFormatException e) {
            throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Source_Request_Identifier", new Object[] {identifier, Request.DELIMITER}); //$NON-NLS-1$
        } catch (ArrayIndexOutOfBoundsException e) {
        	throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Request_Identifier", new Object[] {identifier, Request.DELIMITER}); //$NON-NLS-1$
        }
        
            
        try {
            getQueryServiceProxy().cancelQuery(new AtomicRequestID(new RequestID(sessionID, requestIDLong), nodeID, executionId));
        }  catch(InvalidRequestIDException e) {
        	throw new AdminProcessingException(e);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#startConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void startConnectorBinding(String identifier) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        Collection bindings = parent.getConnectorBindings(identifier);
        if (bindings == null || bindings.size() == 0) {
            throwProcessingException("ServerRuntimeStateAdminImpl.No_Connector_Bindings_Found", new Object[] {identifier});  //$NON-NLS-1$
        }

        try {    
            Collection expectedServiceNames = new HashSet();
            for (Iterator iter = bindings.iterator(); iter.hasNext(); ) {
                MMConnectorBinding binding = (MMConnectorBinding) iter.next();

                if (binding.getState() != ConnectorBinding.STATE_OPEN && 
                    binding.getState() != ConnectorBinding.STATE_DATA_SOURCE_UNAVAILABLE) {
                    
                    String[] identifierParts = binding.getIdentifierArray();
                    String hostName = identifierParts[0];
                    String processName = identifierParts[1];
                    String bindingName = identifierParts[2];
                    
                    expectedServiceNames.addAll(getServiceNamesFromConfiguration(hostName, processName, bindingName));
                    
                    ServiceID serviceID = new ServiceID(binding.getServiceID(), hostName, processName);
                    
                    getRuntimeStateAdminAPIHelper().restartService(serviceID);
                }
            }
            
            
            parent.waitForServicesToStart(expectedServiceNames);
            
        } catch (ConfigurationException e) {
        	throw new AdminComponentException(e);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#startHost(java.lang.String, boolean)
     * @since 4.3
     */
    public void startHost(String hostName, boolean waitUntilDone) throws AdminException {
        if (hostName == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        try {
            //start the host
            getRuntimeStateAdminAPIHelper().startHost(hostName);

            
            //wait until runtime matches the configuration
            if (waitUntilDone) {
                //get the configuration's list of services
                Collection expectedServiceNames = getServiceNamesFromConfiguration(hostName);            	
                parent.waitForServicesToStart(expectedServiceNames);
            }
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }
        
    
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#startProcess(java.lang.String, boolean)
     * @since 4.3
     */
    public void startProcess(String identifier, boolean waitUntilDone) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        String[] identifierParts = null;
		try {
			identifierParts = MMProcess.buildIdentifierArray(identifier);
	        if (identifierParts.length != 2) {
	            throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Process_Identifier", new Object[] {identifier, ProcessObject.DELIMITER});  //$NON-NLS-1$
	        } 			
		} catch (ArrayIndexOutOfBoundsException e) {
			throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Request_Identifier", new Object[] {identifier, Request.DELIMITER}); //$NON-NLS-1$
		} catch(StringIndexOutOfBoundsException e) {
			throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Request_Identifier", new Object[] {identifier, Request.DELIMITER}); //$NON-NLS-1$
		}
        
        String hostName = identifierParts[0];
        String processName = identifierParts[1];
        
        try {                
            //start the process
			getRuntimeStateAdminAPIHelper().startProcess(hostName, processName);
            
            //wait until runtime matches the configuration
            if (waitUntilDone) {
                //get the configuration's list of services
                Collection expectedServiceNames = getServiceNamesFromConfiguration(hostName, processName);
            	
                parent.waitForServicesToStart(expectedServiceNames);
            }        
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }
    
    
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#stopConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void stopConnectorBinding(String identifier, boolean stopNow) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        Collection bindings = parent.getConnectorBindings(identifier);
        if (bindings == null || bindings.size() == 0) {
            throwProcessingException("ServerRuntimeStateAdminImpl.No_Connector_Bindings_Found", new Object[] {identifier});  //$NON-NLS-1$
        }
            
        try {
            Collection expectedServiceNames = new HashSet();
            for (Iterator iter = bindings.iterator(); iter.hasNext(); ) {
                MMConnectorBinding binding = (MMConnectorBinding) iter.next();
                
                String[] identifierParts = binding.getIdentifierArray();
                String hostName = identifierParts[0];
                String processName = identifierParts[1];
                String bindingName = identifierParts[2];
                expectedServiceNames.addAll(getServiceNamesFromConfiguration(hostName, processName, bindingName));
    
                ServiceID serviceID = new ServiceID(binding.getServiceID(), hostName, processName);
                
                getRuntimeStateAdminAPIHelper().stopService(serviceID, stopNow);
            }
                
            
            //wait until runtime matches the configuration
            parent.waitForServicesToStop(expectedServiceNames); 
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }
    
    

    
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#stopHost(java.lang.String, boolean, boolean)
     * @since 4.3
     */
    public void stopHost(String hostName, boolean stopNow, boolean waitUntilDone) throws AdminException {
        if (hostName == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        try {
            getRuntimeStateAdminAPIHelper().stopHost(hostName, stopNow);
            
            if (waitUntilDone) {
                boolean done = false;
                while (! done) {
                    done = isHostStopped(hostName);  
                    try {
						Thread.sleep(ServerAdminImpl.SERVICE_WAIT_INTERVAL);
					} catch (InterruptedException e) {
					}
                }
            }      
        } catch (MultipleException e) {
        	throw new AdminProcessingException(e);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#stopProcess(java.lang.String, boolean, boolean)
     * @since 4.3
     */
    public void stopProcess(String identifier, boolean stopNow, boolean waitUntilDone) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        Collection processes = parent.getProcesses(identifier);
        if (processes == null || processes.size() == 0) {
            throwProcessingException("ServerRuntimeStateAdminImpl.No_Processes_Found", new Object[] {identifier});  //$NON-NLS-1$
        } else if (processes.size() > 1) {
            throwProcessingException("ServerRuntimeStateAdminImpl.Multiple_Processes_Found", new Object[] {identifier});  //$NON-NLS-1$
        }
                
        try {
            MMProcess process = (MMProcess) processes.iterator().next();

            getRuntimeStateAdminAPIHelper().stopProcess(process.getHostName(), process.getProcessName(), stopNow);
            
            if (waitUntilDone) {
                boolean done = false;
                while (! done) {
                    done = isProcessStopped(process.getName(), process.getHostName());
                    try {
						Thread.sleep(ServerAdminImpl.SERVICE_WAIT_INTERVAL);
					} catch (InterruptedException e) {
					}
                }
            }   
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        } catch (AuthorizationException e) {
        	throw new AdminProcessingException(e);
        }
     }
    

    
    /**
	 * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#stopSystem()
	 * @since 4.3
	 */
    public void stopSystem() throws AdminException {
        try {
            getRuntimeStateAdminAPIHelper().shutdownServer();
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);            
        }
    }

    
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#bounceSystem(boolean)
     * @param waitUntilDone Ignored: the waiting for this method is done in ServerAdminClientInterceptor on the client-side.
     * @since 4.3
     */
    public void restart() throws AdminException {   
        try {
            getRuntimeStateAdminAPIHelper().bounceServer();
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }
    
    
    
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#synchronizeSystem(boolean)
     * @since 4.3
     */
    public void synchronizeSystem(boolean waitUntilDone) throws AdminException {
        try {
            //get the configuration's list of services
            Collection expectedServiceNames = getServiceNamesFromConfiguration();
            
            //synchronize
            getRuntimeStateAdminAPIHelper().synchronizeServer();
            
            //wait until runtime matches the configuration
            if (waitUntilDone) {
                parent.waitForServicesToStart(expectedServiceNames);
            }
           
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        } catch (MultipleException e) {
        	throw new AdminProcessingException(e);
        }
    }
    
    
    
    /**
     * @see org.teiid.adminapi.RuntimeStateAdmin#clearCache(java.lang.String)
     * @since 4.3
     */
    public void clearCache(String cacheIdentifier) throws AdminException {
        if (cacheIdentifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }

        if (cacheIdentifier != null
            && (cacheIdentifier.equals(Cache.CODE_TABLE_CACHE)
                || cacheIdentifier.equals(Cache.PREPARED_PLAN_CACHE)
                || cacheIdentifier.equals(Cache.QUERY_SERVICE_RESULT_SET_CACHE) 
                || cacheIdentifier.equals(Cache.CONNECTOR_RESULT_SET_CACHE))) {

            
               List<ServiceRegistryBinding> serviceBindings = this.registry.getServiceBindings(null, null);
               
               for (ServiceRegistryBinding serviceBinding:serviceBindings) {
                    try {
                    	ServiceInterface service = serviceBinding.getService();
                        if (service instanceof CacheAdmin) {
                            CacheAdmin admin = (CacheAdmin)service;
                            Map caches = admin.getCaches(); // key = cache name, value = cache type
                            if (caches != null) {
                                Iterator cacheIter = caches.keySet().iterator();
                                while (cacheIter.hasNext()) {
                                    String cacheName = (String)cacheIter.next();
                                    String cacheType = (String)caches.get(cacheName);
                                    if (cacheType.equals(cacheIdentifier)) {
                                        super.logDetail("ServerRuntimeStateAdminImpl.clearing_cache",new Object[] {cacheName});  //$NON-NLS-1$
                                        admin.clearCache(cacheName, null); // properties not currently used
                                    }
                                }
                            }
                        }
                    } catch (MetaMatrixComponentException e) {
                    	throw new AdminComponentException(e);
                    }
               }
        } else {
            throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_cache_Identifier", new Object[] {cacheIdentifier}); //$NON-NLS-1$
        }
    }
    
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#terminateSession(java.lang.String)
     * @since 4.3
     */
    public void terminateSession(String identifier) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        long sessionIDStringLong = -1;
        try {
            sessionIDStringLong = Long.parseLong(identifier);
        } catch (Exception e) {
            throwProcessingException("ServerRuntimeStateAdminImpl.Invalid_Session_Identifier", new Object[] {identifier});  //$NON-NLS-1$
        } 
        
        try {
        	MetaMatrixSessionID sessionID = new MetaMatrixSessionID(sessionIDStringLong);
			getSessionServiceProxy().terminateSession(sessionID, DQPWorkContext.getWorkContext().getSessionId());
        } catch(IllegalArgumentException e) {
        	throw new AdminProcessingException(e);
		} catch (InvalidSessionException e) {
			throw new AdminProcessingException(e);
		} catch (AuthorizationException e) {
			throw new AdminProcessingException(e);
		} catch (SessionServiceException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
    }
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#changeVDBStatus(java.lang.String, java.lang.String, int)
     * @since 4.3
     */
    public void changeVDBStatus(String name, String version, int newStatus) throws AdminException {
        if (name == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        Collection vdbs = parent.getVDBs(name);
        if (vdbs == null || vdbs.size() == 0) {
            throwProcessingException("ServerRuntimeStateAdminImpl.No_VDBs_Found", new Object[] {name});  //$NON-NLS-1$
        } 
        
        boolean found = false;
        MMVDB vdb = null;
        for (Iterator iter = vdbs.iterator(); iter.hasNext();) {
            vdb = (MMVDB) iter.next();
            if (vdb.getVDBVersion().equals(version)) {
                found = true;
                break;
            }   
        }
        
        if (! found) {
            throwProcessingException("ServerRuntimeStateAdminImpl.No_VDBs_Found_With_Version", new Object[] {name, version});  //$NON-NLS-1$
        }
        
        
        VirtualDatabaseID vdbID = new BasicVirtualDatabaseID(name, version, vdb.getUID());
        int currentStatus = vdb.getState();
        
        //------------------
        // DELETE TRANSITION
        //------------------
        // Can only change state to DELETED from states INACTIVE or INCOMPLETE.
        if ( newStatus == VDBStatus.DELETED ) {
            try {
                if (currentStatus != VDBStatus.INACTIVE && 
                    currentStatus != VDBStatus.INCOMPLETE) {
                    // Go to INACTIVE status first
                    try {
                        RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, VDBStatus.INACTIVE, getUserName());
                        RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, VDBStatus.DELETED, getUserName());
                    } catch (VirtualDatabaseException e) {
                    	throw new AdminProcessingException(e);
                    }
                } else if ( currentStatus != VDBStatus.DELETED ) {
                    // don't delete if already marked for delete
                    try {
                        RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, VDBStatus.DELETED, getUserName());
                    } catch (VirtualDatabaseException e) {
                    	throw new AdminProcessingException(e);
                    }
                }
            } finally {
                // Attempt to delete it if no one is using it.
                RuntimeVDBDeleteUtility vdbDeleter = new RuntimeVDBDeleteUtility();
                try {
                    vdbDeleter.deleteVDBMarkedForDelete(vdbID);
                } catch (VirtualDatabaseException e) {
                	throw new AdminProcessingException(e);
                } catch (MetaMatrixComponentException e) {
                	throw new AdminComponentException(e);
                }
            }
        } else {
            //------------------
            // Other TRANSITION
            //------------------
            try {
                RuntimeMetadataCatalog.getInstance().setVDBStatus(vdbID, (short) newStatus, getUserName());
            } catch (VirtualDatabaseException e) {
            	throw new AdminProcessingException(e);
            }
        }
        
    }
    
    
    
    
    
    
    /**
     * @return Collection<String> The full-names of services known to the configuration.
     * @since 4.3
     */
    private Collection getServiceNamesFromConfiguration() throws MetaMatrixComponentException {
        Collection expectedServiceNames = new ArrayList();            
        Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
        Collection components = config.getDeployedComponents();
        for (Iterator iter = components.iterator(); iter.hasNext();) {
            BasicDeployedComponent component = (BasicDeployedComponent)iter.next();
            expectedServiceNames.add(component.getID().getFullName());                 
        }
        return expectedServiceNames;
    }
    
    /**
     * @param hostName Host to look for the specified process on.
     * @return Collection<String> The full-names of services known to the configuration for the specified host.
     * @since 4.3
     */
    private Collection getServiceNamesFromConfiguration(String hostName) throws MetaMatrixComponentException {
        Collection expectedServiceNames = new ArrayList();            
        Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
        Collection components = config.getDeployedComponents();
        for (Iterator iter = components.iterator(); iter.hasNext();) {
            BasicDeployedComponent component = (BasicDeployedComponent)iter.next();
            if (component.getHostID().getName().equalsIgnoreCase(hostName)) {
                expectedServiceNames.add(component.getID().getFullName());
            }            
        }
        return expectedServiceNames;
    }
    
    
    /**
     * @param hostName Host to look for the specified process on.
     * @param processName Process name to return services for.
     * @return Collection<String> The full-names of services known to the configuration for the specified process
     * on the specified host.
     * @since 4.3
     */
    private Collection getServiceNamesFromConfiguration(String hostName, String processName) throws MetaMatrixComponentException {
        Collection expectedServiceNames = new ArrayList();            
        Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
        Collection components = config.getDeployedComponents();
        for (Iterator iter = components.iterator(); iter.hasNext();) {
            BasicDeployedComponent component = (BasicDeployedComponent)iter.next();
            if (component.getHostID().getName().equalsIgnoreCase(hostName) &&
                component.getVMComponentDefnID().getName().equalsIgnoreCase(processName)) {
                expectedServiceNames.add(component.getID().getFullName());
            }            
        }
        return expectedServiceNames;
    }
    
    
    /**
     * @param hostName Host to look for the specified process on.
     * @param processName Process name to return services for.
     * @param serviceName Service name to return services for.
     * @return Collection<String> The full-names of services known to the configuration for the specified serviceName,
     * on the specified process and host.
     * @since 4.3
     */
    private Collection getServiceNamesFromConfiguration(String hostName, String processName, String serviceName) throws ConfigurationException {
        Collection expectedServiceNames = new ArrayList();            
        Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
        Collection components = config.getDeployedComponents();
        for (Iterator iter = components.iterator(); iter.hasNext();) {
            BasicDeployedComponent component = (BasicDeployedComponent)iter.next();
            if (component.getHostID().getName().equalsIgnoreCase(hostName) &&
                component.getVMComponentDefnID().getName().equalsIgnoreCase(processName) &&
                component.getID().getName().equalsIgnoreCase(serviceName)) {
                expectedServiceNames.add(component.getID().getFullName());
            }            
        }
        return expectedServiceNames;        
    }
    
    
    
    
    
    
      
    
    
    /**
     * @return true if the host is unknown to the registry, or if it is in the "unregistered" state.
     * @since 4.3
     */
    private boolean isProcessStopped(String hostName, String processName) throws MetaMatrixComponentException {    
        SystemState systemState = getRuntimeStateAdminAPIHelper().getSystemState();
        Collection hostDatas = systemState.getHosts();
        
        for (Iterator iter = hostDatas.iterator(); iter.hasNext();) {
            HostData hostData = (HostData) iter.next();       
            
            if (hostData.getName().equalsIgnoreCase(hostName)) {
                Collection processDatas = hostData.getProcesses();
            
                for (Iterator iter2 = processDatas.iterator(); iter2.hasNext();) {
                    ProcessData processData = (ProcessData) iter2.next();
                    
                    if (processData.getName().equalsIgnoreCase(processName)) {
                        if (processData.isRegistered()) {
                            return false;
                        }
                    }
                }
            }
        }
        
        return true;
    }
    
    /**
     * @return true if the host is unknown to the registry, or if it is in the "unregistered" state.
     * @since 4.3
     */
    private boolean isHostStopped(String hostName) throws MetaMatrixComponentException {    
        SystemState systemState = getRuntimeStateAdminAPIHelper().getSystemState();
        Collection hostDatas = systemState.getHosts();
        
        for (Iterator iter = hostDatas.iterator(); iter.hasNext();) {
            HostData hostData = (HostData) iter.next();
            
            if (hostData.getName().equalsIgnoreCase(hostName)) {
                Collection processDatas = hostData.getProcesses();
                for (Iterator iter2 = processDatas.iterator(); iter2.hasNext();) {
                    ProcessData processData = (ProcessData) iter2.next();                    
                    if (processData.isRegistered()) {
                        return false;                        
                    }
                }
            }
        }
        
        return true;
    }
    
    @Override
    public void terminateTransaction(String transactionId, String sessionId)
    		throws AdminException {
    	this.getQueryServiceProxy().terminateTransaction(transactionId, sessionId);
    }
    
    @Override
    public void terminateTransaction(Xid transactionId) throws AdminException {
    	this.getQueryServiceProxy().terminateTransaction(transactionId);
    }

	@Override
	public void setLogListener(EmbeddedLogger listener) throws AdminException {
	}

	@Override
	public void bounceSystem(boolean waitUntilDone) throws AdminException {
	}

	@Override
	public void shutdown(int millisToWait) throws AdminException {
	}
}