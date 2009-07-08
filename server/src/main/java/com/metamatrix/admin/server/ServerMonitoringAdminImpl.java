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

import static org.teiid.dqp.internal.process.Util.convertStats;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Resource;
import org.teiid.adminapi.Service;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.SystemObject;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;

import com.metamatrix.admin.api.server.ServerMonitoringAdmin;
import com.metamatrix.admin.objects.MMAdminObject;
import com.metamatrix.admin.objects.MMConnectionPool;
import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMConnectorType;
import com.metamatrix.admin.objects.MMDQP;
import com.metamatrix.admin.objects.MMExtensionModule;
import com.metamatrix.admin.objects.MMHost;
import com.metamatrix.admin.objects.MMProcess;
import com.metamatrix.admin.objects.MMQueueWorkerPool;
import com.metamatrix.admin.objects.MMRequest;
import com.metamatrix.admin.objects.MMResource;
import com.metamatrix.admin.objects.MMService;
import com.metamatrix.admin.objects.MMSession;
import com.metamatrix.admin.objects.MMSystem;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.common.config.api.ComponentObject;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicDeployedComponent;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.ZipFileUtil;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.SystemState;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.MetaMatrixSessionState;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.ProductInfoConstants;
import com.metamatrix.platform.vm.controller.ProcessStatistics;
import com.metamatrix.platform.vm.controller.SocketListenerStats;
import com.metamatrix.server.serverapi.RequestInfo;
/**
 * @since 4.3
 */
public class ServerMonitoringAdminImpl extends AbstractAdminImpl implements ServerMonitoringAdmin {

    private static final String QUERY_SERVICE = "QueryService"; //$NON-NLS-1$
    
    public ServerMonitoringAdminImpl(ServerAdminImpl parent, ClusteredRegistryState registry) {
        super(parent, registry);
    }

    /**    
     * Get monitoring information about caches.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getCaches(java.lang.String)
     * @param identifier Identifier of the cache to get information for.  For example, "CodeTable".  
     * <p>If identifier is "*", this method returns information for all caches in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.Cache<code>
     * @since 4.3
     */
    public Collection getCaches(String identifier) throws AdminException  {
        //todo: [P2]
        return null;
    }
    
    /**
     * Get monitoring information about connector bindings.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getConnectorBindings(java.lang.String)
     * @param identifier Fully-qualified identifier of a host, process, or connector binding
     * to get information for.  For example, "hostname", or "hostname.processname", or "
     * "hostname.processname.bindingname". 
     * <p>If identifier is "*", this method returns information about all connector bindings in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.ConnectorBinding</code>
     * @since 4.3
     */
    public Collection getConnectorBindings(String identifier) throws AdminException  {
        
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        HashSet results = new HashSet();
        try {
			//get config data from ConfigurationService
			Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
			Collection components = config.getDeployedComponents();
			
			//convert config data to MMConnectorBinding objects, put in a hashmap by identifier
			Map runtimeMap = new HashMap();
			for (Iterator iter = components.iterator(); iter.hasNext();) {
			    BasicDeployedComponent component = (BasicDeployedComponent)iter.next();
			    if (!component.isDeployedConnector()) {
			    	continue;
			    }
			    String bindingName = component.getName();

			    String[] identifierParts = new String[] {
			        component.getHostID().getName(), component.getVMComponentDefnID().getName(), bindingName
			    };

			    ConnectorBinding configBinding = config.getConnectorBinding(bindingName);
			    if (configBinding != null && identifierMatches(identifier, identifierParts)) {

			        MMConnectorBinding binding = new MMConnectorBinding(identifierParts);
			        binding.setConnectorTypeName(configBinding.getComponentTypeID().getFullName());
			        binding.setRoutingUUID(configBinding.getRoutingUUID());
			        binding.setEnabled(component.isEnabled()); // use the deployed component setting for being enabled.
			        binding.setRegistered(false);
			        binding.setState(MMConnectorBinding.STATE_NOT_REGISTERED);
			        binding.setProperties(configBinding.getProperties());
			        binding.setDescription(component.getDescription());
			        
			        binding.setCreated(configBinding.getCreatedDate());
			        binding.setCreatedBy(configBinding.getCreatedBy());
			        binding.setLastUpdated(configBinding.getLastChangedDate());
			        binding.setLastUpdatedBy(configBinding.getLastChangedBy());

			        runtimeMap.put(component.getFullName(), binding);
			        results.add(binding);
			    }

			}
			
			// get runtime data from RuntimeStateAdminAPIHelper
			Collection serviceBindings = this.registry.getServiceBindings(null, null);
			
			//convert runtime data into MMConnectorBinding objects
			for (Iterator iter = serviceBindings.iterator(); iter.hasNext();) {
			    ServiceRegistryBinding serviceBinding = (ServiceRegistryBinding) iter.next();
			    DeployedComponent deployedComponent = serviceBinding.getDeployedComponent();

			    if (deployedComponent!= null && deployedComponent.isDeployedConnector()) {
			        String name = serviceBinding.getDeployedName();
			        
			        MMConnectorBinding binding;
			        if (runtimeMap.containsKey(deployedComponent.getFullName())) {
			            //reuse MMConnectorBinding from config
			            binding = (MMConnectorBinding) runtimeMap.get(deployedComponent.getFullName());
			            binding.setState(serviceBinding.getCurrentState());
			            binding.setStateChangedTime(serviceBinding.getStateChangeTime());
			            binding.setRegistered(true);
			            binding.setServiceID(serviceBinding.getServiceID().getID());

			        } else {
			        	
					    String[] identifierParts = new String[] {
						        deployedComponent.getHostID().getName(), 
						        deployedComponent.getVMComponentDefnID().getName(), 
						        deployedComponent.getName()
						    };
				            
				        if (identifierMatches(identifier, identifierParts)) {
				        	
				            //not in config - create new MMConnectorBinding
				            binding = new MMConnectorBinding(identifierParts);
				            binding.setState(MMConnectorBinding.STATE_NOT_DEPLOYED);

	
				            binding.setConnectorTypeName(deployedComponent.getComponentTypeID().getFullName());
				            binding.setDescription(deployedComponent.getDescription());
				            binding.setState(serviceBinding.getCurrentState());
				            binding.setStateChangedTime(serviceBinding.getStateChangeTime());
				            binding.setRegistered(true);
				            binding.setServiceID(serviceBinding.getServiceID().getID());
				            
				            results.add(binding);
			        	
				        }
			        	
			        }
			    }
			}
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		}
        return results;
    }

    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getConnectorBindingsInVDB(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindingsInVDB(String identifier) throws AdminException {
        
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        Set allBindingNames = new HashSet();
        Collection vdbs = this.getVDBs(identifier);
        
        // First get all connector binding names from all VDBs with
        // the given identifier
        Iterator vdbItr = vdbs.iterator();
        while ( vdbItr.hasNext() ) {
            VDB aVdb = (VDB)vdbItr.next();
            
            Collection models = aVdb.getModels();
            Iterator modelItr = models.iterator();
            while ( modelItr.hasNext() ) {
                Model aModel = (Model)modelItr.next();
                List bindingNames = aModel.getConnectorBindingNames();
                if ( bindingNames != null && bindingNames.size() > 0 ) {
                    allBindingNames.addAll(bindingNames);
                }
            }
        }
        
        // Using collected connector binding names, get all connector bindings
        Collection connectorBindings = new ArrayList();
        
        Iterator bindingNameItr = allBindingNames.iterator();
        while ( bindingNameItr.hasNext() ) {
            Collection cb = getConnectorBindings(AdminObject.WILDCARD + (String)bindingNameItr.next());
            connectorBindings.addAll(cb);
        }
        return connectorBindings;
    }    
    
    /**
     * Get monitoring information about connector types.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getConnectorTypes(java.lang.String)
     * @param identifier Identifier of a connector type to get information for.  For example, "JDBC Connector". 
     * <p>If identifier is "*", this method returns information about all connector types in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.ConnectorType</code>
     * @since 4.3
     */
    public Collection getConnectorTypes(String identifier) throws AdminException  {
        
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        ArrayList results = null;
        try {
			//get types from ConfigurationService
			Collection types = getConfigurationServiceProxy().getAllComponentTypes(false);
			
			//convert results into MMConnectorType objects
			results = new ArrayList(types.size());
			for (Iterator iter = types.iterator(); iter.hasNext();) {
			    ComponentType componentType = (ComponentType) iter.next();
			    if (componentType.getComponentTypeCode() == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE) {
			        
			        String name = componentType.getName();
			        String[] identifierParts = new String[] {name};                
			        if (identifierMatches(identifier, identifierParts)) {
			            MMConnectorType type = new MMConnectorType(identifierParts);
			            
			            type.setCreated(componentType.getCreatedDate());
			            type.setCreatedBy(componentType.getCreatedBy());
			            type.setLastUpdated(componentType.getLastChangedDate());
			            type.setLastUpdatedBy(componentType.getLastChangedBy());
			                
			            results.add(type);
			        }
			    }
			}
			return results;
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
    }

    /**
     * Get monitoring information about DQPs (Distributed Query Processors).
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getDQPs(java.lang.String)
     * @param identifier Fully-qualified identifier of a host or process
     * to get information for.  For example, "hostname", or "hostname.processname". 
     * <p>If identifier is "*", this method returns information about all DQPs in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.DQP</code>
     * @since 4.3
     */
    public Collection getDQPs(String identifier) throws AdminException  {
        
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        HashSet results = new HashSet();
        try {
			//get config data from ConfigurationService
			
			Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
			Collection components = config.getDeployedComponents();
			
			//convert config data to MMConnectorBinding objects, put in a hashmap by identifier
			Map runtimeMap = new HashMap();
			for (Iterator iter = components.iterator(); iter.hasNext();) {
			    BasicDeployedComponent component = (BasicDeployedComponent) iter.next();
			    
			    String dqpName = component.getName();
			    String[] identifierParts = 
			        new String[] {component.getHostID().getName(), component.getVMComponentDefnID().getName(), dqpName};
			    if (QUERY_SERVICE.equals(component.getComponentTypeID().getName()) 
			                    && identifierMatches(identifier, identifierParts)) {

			        MMDQP dqp = new MMDQP(identifierParts);
			        
			        dqp.setRegistered(false);
			        dqp.setState(MMDQP.STATE_NOT_REGISTERED);
			        
			        ServiceComponentDefn defn = config.getServiceComponentDefn(dqpName);                    
			        if (defn != null) {
			            dqp.setProperties(defn.getProperties());
			        }
			        
			        dqp.setEnabled(component.isEnabled());
			        
			        String key = MMAdminObject.buildIdentifier(identifierParts).toUpperCase();
			        runtimeMap.put(key, dqp);
			        results.add(dqp);
			    }
			}
			
			
			
			//get runtime data from RuntimeStateAdminAPIHelper
			Collection serviceBindings = this.registry.getServiceBindings(null, null);
			
			//convert runtime data into MMDQP objects
			for (Iterator iter = serviceBindings.iterator(); iter.hasNext();) {
			    ServiceRegistryBinding serviceBinding = (ServiceRegistryBinding) iter.next();
			    DeployedComponent deployedComponent = serviceBinding.getDeployedComponent();

			    if (QUERY_SERVICE.equals(serviceBinding.getServiceType())) {
			        String name = serviceBinding.getDeployedName();
			        
			        MMDQP dqp;
			        String[] identifierParts = 
			            new String[] {serviceBinding.getHostName(), serviceBinding.getProcessName(), name};
			        String key = MMAdminObject.buildIdentifier(identifierParts).toUpperCase();
			        if (runtimeMap.containsKey(key)) {
			            //reuse MMDQP from config
			            dqp = (MMDQP) runtimeMap.get(key);
			        } else {
			            //not in config - create new MMDQP
			            dqp = new MMDQP(identifierParts);
			            dqp.setState(MMDQP.STATE_NOT_DEPLOYED);
			        }
			            
			        if (identifierMatches(identifier, identifierParts)) {
			                                    
			            dqp.setCreated(deployedComponent.getCreatedDate());
			            dqp.setCreatedBy(deployedComponent.getCreatedBy());
			            dqp.setLastUpdated(deployedComponent.getLastChangedDate());
			            dqp.setLastUpdatedBy(deployedComponent.getLastChangedBy());
			            dqp.setDescription(deployedComponent.getDescription());
			            dqp.setState(serviceBinding.getCurrentState());
			            dqp.setStateChangedTime(serviceBinding.getStateChangeTime());
			            dqp.setRegistered(true);
			            dqp.setServiceID(serviceBinding.getServiceID().getID());
			            
			            results.add(dqp);
			        }
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
        return results;
    }

    
    /**
     * Get monitoring information about extension modules.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getExtensionModules(java.lang.String)
     * @param identifier Identifier of an extension module to get information for.   
     * <p>If identifier is "*", this method returns information about all extension modules in the system.
     * Note that this methods returns the binary contents of each extension module, so you 
     * should avoid calling this method with identifier "*" if possible.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.ExtensionModule</code>
     * @since 4.3
     */
    public Collection getExtensionModules(String identifier) throws AdminException  {

        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        ArrayList results = null;
        try {
			//get modules from ExtensionSourceManager
			Collection modules = getExtensionSourceManager().getSourceDescriptors();
			
			//convert modules into MMExtensionModule objects
			results = new ArrayList(modules.size());
			for (Iterator iter = modules.iterator(); iter.hasNext();) {
			    ExtensionModuleDescriptor descriptor = (ExtensionModuleDescriptor) iter.next();
			    String sourceName = descriptor.getName();
			    
			    String[] identifierParts = new String[] {sourceName};                
			    if (identifierMatches(identifier, identifierParts)) {
			        MMExtensionModule module = new MMExtensionModule(identifierParts);
			        
			        module.setModuleType(descriptor.getType());
			        module.setDescription(descriptor.getDescription());
			        module.setEnabled(descriptor.isEnabled());
			        byte[] contents = getExtensionSourceManager().getSource(sourceName);
			        module.setFileContents(contents);
			        module.setCreated(DateUtil.convertStringToDate(descriptor.getCreationDate()));
			        module.setCreatedBy(descriptor.getCreatedBy());
			        module.setLastUpdated(DateUtil.convertStringToDate(descriptor.getLastUpdatedDate()));
			        module.setLastUpdatedBy(descriptor.getLastUpdatedBy());
			                
			        results.add(module);
			    }
			}
		} catch (ExtensionModuleNotFoundException e) {
			throw new AdminProcessingException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		} catch (ParseException e) {
			throw new AdminComponentException(e);
		}
        return results;
    }

    /**
     * Get monitoring information about hosts.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getHosts(java.lang.String)
     * @param identifier Identifier of a host to get information for.  For example "hostname".  
     * <p>If identifier is "*", this method returns information about all hosts in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.Host</code>
     * @since 4.3
     */
    public Collection getHosts(String identifier) throws AdminException  {

        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        HashSet results = new HashSet();               
        try {
			//get runtime data from RuntimeStateAdminAPIHelper
			SystemState systemState = getRuntimeStateAdminAPIHelper().getSystemState();
			Collection hostDatas = systemState.getHosts();
			
			//convert runtime data to MMHost objects, put in a hashmap by hostName
			Map runtimeMap = new HashMap();
			for (Iterator iter = hostDatas.iterator(); iter.hasNext();) {
			    HostData hostData = (HostData) iter.next();
			    
			    String hostName = hostData.getName();
			    String[] identifierParts = new String[] {hostName};                
			    if (identifierMatches(identifier, identifierParts)) {
			        MMHost host = new MMHost(identifierParts);
			        
			        host.setRunning(hostData.isRegistered());
			        host.setRegistered(hostData.isRegistered());
			        host.setProperties(hostData.getProperties());

			        runtimeMap.put(hostName.toUpperCase(), host);
			        results.add(host);
			    }
			}
			
			
			//get config data from ConfigurationServiceProxy
			Collection hosts = getConfigurationServiceProxy().getHosts();
			    
			//convert config data to MMHost objects, merge with runtime data, 
			for (Iterator iter = hosts.iterator(); iter.hasNext();) {
			    Host hostObject = (Host) iter.next();
			    String hostName = hostObject.getName();
			    
			    MMHost host;
			    if (runtimeMap.containsKey(hostName.toUpperCase())) {
			        //reuse MMHost from runtime
			        host = (MMHost) runtimeMap.get(hostName.toUpperCase());                    
			    } else {
			        //not in runtime: create new MMHost
			        String[] identifierParts = new String[] {hostName};                
			        host = new MMHost(identifierParts);
			        host.setRunning(false);
			        host.setRegistered(false);
			    }
			    
			    
			    if (identifierMatches(identifier, host.getIdentifierArray())) {
			        host.setCreated(hostObject.getCreatedDate());
			        host.setCreatedBy(hostObject.getCreatedBy());
			        host.setLastUpdated(hostObject.getLastChangedDate());
			        host.setLastUpdatedBy(hostObject.getLastChangedBy());
			        host.setEnabled(hostObject.isEnabled());
			        
			        Properties properties = hostObject.getProperties();
			        if (host.getProperties() != null) {
			        	host.getProperties().putAll(properties);
			        }
			        else {
			        	host.setProperties(properties);
			        }
			        
			        results.add(host);
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
        return results;
    }

    /**
     * Get monitoring information about processes.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getProcesses(java.lang.String)
     * @param identifier Identifier of a host or process to get information for.
     * For example "hostname" or "hostname.processname".   
     * <p>If identifier is "*", this method returns information about all processes in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.Process</code>
     * @since 4.3
     */
   public Collection getProcesses(String identifier) throws AdminException  {

       if (identifier == null) {
           throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
       }
       
       HashSet results = new HashSet();
       try {
		//get runtime data from RuntimeStateAdminAPIHelper
		   SystemState systemState = getRuntimeStateAdminAPIHelper().getSystemState();
		   Collection hostDatas = systemState.getHosts();
		   
		   //convert runtime data to MMProcess objects, put in hashmap by identifier
		   Map runtimeMap = new HashMap();
		   for (Iterator iter = hostDatas.iterator(); iter.hasNext();) {
		       HostData hostData = (HostData) iter.next();
		       Collection processDatas = hostData.getProcesses();
		       for (Iterator iter2= processDatas.iterator(); iter2.hasNext();) {
		           ProcessData processData = (ProcessData) iter2.next();
		           String processName = processData.getName();
		           String hostName = hostData.getName();
		           
		           String[] identifierParts = new String[] {hostName, processName};                
		           if (identifierMatches(identifier, identifierParts)) {
		               MMProcess process = new MMProcess(identifierParts);
		               process.setRunning(processData.isRegistered());

		               if (processData.isRegistered()) {

		                   try {
		                       ProcessStatistics statistics = getRuntimeStateAdminAPIHelper().getVMStatistics(hostName, processName);
		                       if (statistics != null) {
		                           process.setFreeMemory(statistics.freeMemory);
		                           process.setTotalMemory(statistics.totalMemory);
		                           process.setThreadCount(statistics.threadCount);
		                               
		                           SocketListenerStats socketStats = statistics.socketListenerStats;
		                           if (socketStats != null) {
		                               process.setSockets(socketStats.sockets);
		                               process.setMaxSockets(socketStats.maxSockets);
		                               process.setObjectsRead(socketStats.objectsRead);
		                               process.setObjectsWritten(socketStats.objectsWritten);
		                           }
		                               
		                           WorkerPoolStats workerStats = statistics.processPoolStats;
		                           if (workerStats != null) {
		                               MMQueueWorkerPool workerPool = convertStats(workerStats, hostName, processName, workerStats.name);
		                               
		                               process.setQueueWorkerPool(workerPool);
		                           }
		                       }
			            	   process.setInetAddress(getRuntimeStateAdminAPIHelper().getVMHostName(hostName, processName));
		                   } catch (MetaMatrixComponentException e) {
		                       //do nothing: sometimes when the process is just starting the RMI stub
		                       //for SocketVMController is not initialized yet
		                   }
		               }
		               
		               String key = MMAdminObject.buildIdentifier(identifierParts).toUpperCase();
		               runtimeMap.put(key, process);
		               results.add(process);
		           }
		       }                   
		   }
		   
		   //get config data from ConfigurationServiceProxy
		   Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
		   
		   Collection defns = config.getVMComponentDefns();
		   //convert config data to MMProcess objects, merge with runtime data
		   for (Iterator iter = defns.iterator(); iter.hasNext();) {
		       VMComponentDefn defn = (VMComponentDefn) iter.next();               
		       String processName = defn.getName();
		       Host h = config.getHost(defn.getHostID().getName());
		       
		       String[] identifierParts = new String[] {h.getName(), processName};
		       String key = MMAdminObject.buildIdentifier(identifierParts).toUpperCase();
		       
		       MMProcess process;               
		       if (runtimeMap.containsKey(key)) {
		           //reuse MMProcess from runtime
		           process = (MMProcess) runtimeMap.get(key);                    
		       } else {
		           //not in runtime: create new MMProcess
		           process = new MMProcess(identifierParts);
		           process.setRunning(false);
		       }
		       
		       
		       if (identifierMatches(identifier, process.getIdentifierArray())) {
		           process.setCreated(defn.getCreatedDate());
		           process.setCreatedBy(defn.getCreatedBy());
		           process.setLastUpdated(defn.getLastChangedDate());
		           process.setLastUpdatedBy(defn.getLastChangedBy());
		           process.setProperties(defn.getProperties());
		           process.setEnabled(defn.isEnabled());
		           
		           String portString = defn.getPort();
		           if( portString != null ) {
		               process.setPort(Integer.parseInt(portString));
		           }
		           if (process.getInetAddress() == null) {
			           try {
							process.setInetAddress(InetAddress.getByName(h.getHostAddress()));
					   } catch (UnknownHostException e) {
							throw new AdminComponentException(e);
					   }
		           }
		           results.add(process);
		       }
		   }
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
       return results;
    }
   
   /**
    * Get monitoring information about services.
    * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getServicess(java.lang.String)
    * @param identifier Fully-qualified identifier of a host, process, or service
    * to get information for.  For example, "hostname", or "hostname.processname", or "
    * "hostname.processname.servicename". 
    * <p>If identifier is "*", this method returns information about all services in the system.
    * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.Service</code>
    * @since 4.3
    */
   public Collection getServices(String identifier) throws AdminException  {
       
       if (identifier == null) {
           throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
       }
       
       HashSet results = new HashSet();
       try {
			//get config data from ConfigurationService
			Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
			Collection components = config.getDeployedComponents();
			
			//convert config data to MMService objects, put in a hashmap by identifier
			Map runtimeMap = new HashMap();
			for (Iterator iter = components.iterator(); iter.hasNext();) {
			    BasicDeployedComponent component = (BasicDeployedComponent)iter.next();
			    if (component.isDeployedConnector()) {
			    	continue;
			    }
			    String serviceName = component.getName();

			    String[] identifierParts = new String[] {
			        component.getHostID().getName(), component.getVMComponentDefnID().getName(), serviceName
			    };

			    ServiceComponentDefn service = config.getServiceComponentDefn(serviceName);
			    if (service != null && identifierMatches(identifier, identifierParts)) {

			    	MMService mmservice = new MMService(identifierParts);
			        mmservice.setComponentTypeName(service.getComponentTypeID().getFullName());
			        mmservice.setEnabled(component.isEnabled());
			        mmservice.setRegistered(false);
			        mmservice.setState(Service.STATE_NOT_REGISTERED);
			        mmservice.setProperties(service.getProperties());
			        mmservice.setDescription(component.getDescription());
			        
			        mmservice.setCreated(service.getCreatedDate());
			        mmservice.setCreatedBy(service.getCreatedBy());
			        mmservice.setLastUpdated(service.getLastChangedDate());
			        mmservice.setLastUpdatedBy(service.getLastChangedBy());

			        runtimeMap.put(component.getFullName(), mmservice);
			        results.add(mmservice);
			    }

			}
			
			// get runtime data from RuntimeStateAdminAPIHelper
			Collection serviceBindings = this.registry.getServiceBindings(null, null);
			
			//convert runtime data into MMConnectorBinding objects
			for (Iterator iter = serviceBindings.iterator(); iter.hasNext();) {
			    ServiceRegistryBinding serviceBinding = (ServiceRegistryBinding) iter.next();
			    DeployedComponent deployedComponent = serviceBinding.getDeployedComponent();

			    if (deployedComponent!= null && ! deployedComponent.isDeployedConnector()) {
			        String name = serviceBinding.getDeployedName();
			        
			        MMService mmservice;
			        if (runtimeMap.containsKey(deployedComponent.getFullName())) {
			            //reuse MMService from config
			            mmservice = (MMService) runtimeMap.get(deployedComponent.getFullName());			            
			            mmservice.setState(serviceBinding.getCurrentState());
			            mmservice.setStateChangedTime(serviceBinding.getStateChangeTime());
			            mmservice.setRegistered(true);
			            mmservice.setServiceID(serviceBinding.getServiceID().getID());

			        } else {
			        	
					    String[] identifierParts = new String[] {
						        deployedComponent.getHostID().getName(), 
						        deployedComponent.getVMComponentDefnID().getName(), 
						        deployedComponent.getName()
						    };
				            
				        if (identifierMatches(identifier, identifierParts)) {
				        	
				            //not in config - create new MMConnectorBinding
				            mmservice = new MMService(identifierParts);
				            mmservice.setState(MMConnectorBinding.STATE_NOT_DEPLOYED);

	
				            mmservice.setComponentTypeName(deployedComponent.getComponentTypeID().getFullName());
				            mmservice.setDescription(deployedComponent.getDescription());
				            mmservice.setState(serviceBinding.getCurrentState());
				            mmservice.setStateChangedTime(serviceBinding.getStateChangeTime());
				            mmservice.setRegistered(true);
				            mmservice.setServiceID(serviceBinding.getServiceID().getID());
				            
				            results.add(mmservice);
			        	
				        }
			        	
			        }
			    }
			}
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		}
       return results;
   }
   

   /**
    * Get monitoring information about worker queues for DQPs or connector bindings.
    * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getQueueWorkerPools(java.lang.String)
    * @param identifier Identifier of a host, process, DQP, connector binding,
    * or worker queue to get information for.
    * For example "hostname", or "hostname.processname", or "hostname.processname.dqpname", or
    * "hostname.processname.bindingname", or "hostname.processname.dqpname.workerqueuename",
    * or "hostname.processname.bindingname.workerqueuename".
    * <p>NOTE: to get information about the "Socket Worker" queue associated with a process,
    * use <code>getProcess()</code>.    
    * <p>If identifier is "*", this method returns information about all worker queues in the system.
    * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.QueueWorkerPool</code>
    * @since 4.3
    */
    public Collection getQueueWorkerPools(String identifier) throws AdminException  {
        
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        ArrayList results = null;
        try {
			//get pools from RuntimeStateAdminAPIHelper
			Collection serviceBindings = this.registry.getServiceBindings(null, null);

			//convert runtime data into MMQueueStatistics objects
			results = new ArrayList(serviceBindings.size());
			for (Iterator iter = serviceBindings.iterator(); iter.hasNext();) {
			    ServiceRegistryBinding binding = (ServiceRegistryBinding) iter.next();
			    DeployedComponent component = binding.getDeployedComponent();
			    
			    if (component.isDeployedConnector() ||
			        QUERY_SERVICE.equals(component.getComponentTypeID().getName())) {
			                
			        Collection statsCollection = getRuntimeStateAdminAPIHelper().getServiceQueueStatistics(binding);
			        
			        for (Iterator iter2 = statsCollection.iterator(); iter2.hasNext();) {
			            WorkerPoolStats stats = (WorkerPoolStats) iter2.next();
			            String name = stats.name;
			            String[] identifierParts = new String[] {binding.getHostName(), 
			                component.getVMComponentDefnID().getName(), 
			                binding.getDeployedName(),
			                name};                
			            if (identifierMatches(identifier, identifierParts)) {
			                MMQueueWorkerPool pool = convertStats(stats, identifierParts);
			                results.add(pool);
			            }
			        }
			    }
			}
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
        return results;
    }

    
    /**
     * Get monitoring information about connection pool stats for the connector bindings.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getConnectionPoolStats(java.lang.String)
     * @param identifier Identifier of a host, process, or connector binding to get information for.
     * For example "hostname", or "hostname.processname", or "hostname.processname.bindingname"
     * <p>If identifier is "*", this method returns information about all connection pools in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.ConnectionPool</code>
     * @since 6.1
     */
     public Collection getConnectionPoolStats(String identifier) throws AdminException  {
         
         if (identifier == null) {
             throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
         }
         
         ArrayList results = null;
         try {
 			//get pools from RuntimeStateAdminAPIHelper
 			Collection serviceBindings = this.registry.getServiceBindings(null, null);

 			//convert runtime data into MMQueueStatistics objects
 			results = new ArrayList(serviceBindings.size());
 			for (Iterator iter = serviceBindings.iterator(); iter.hasNext();) {
 			    ServiceRegistryBinding binding = (ServiceRegistryBinding) iter.next();
 			    DeployedComponent component = binding.getDeployedComponent();
 			    
 			    if (component.isDeployedConnector()) {
			            String[] identifierParts = new String[] {binding.getHostName(), 
	 			                component.getVMComponentDefnID().getName(), 
	 			                component.getServiceComponentDefnID().getFullName()};                
 			            if (identifierMatches(identifier, identifierParts)) {
 			            	 			                
 		 			        Collection statsCollection = getRuntimeStateAdminAPIHelper().getConnectionPoolStats(binding);
 		 			        if (statsCollection != null) {
	 		 			        for (Iterator iter2 = statsCollection.iterator(); iter2.hasNext();) {
	 		 			        	ConnectionPoolStats stats = (ConnectionPoolStats) iter2.next();
	 		 			        	
	 		 			        	MMConnectionPool mmstats = new MMConnectionPool();
	 		 			        	
	 		 			        	mmstats.setConnectorBindingName(component.getServiceComponentDefnID().getFullName());
	 		 			        	mmstats.setConnectorBindingIdentifier(component.getFullName());
	 		 			        	
	 		 			        	mmstats.setConnectionsInUse(stats.getConnectionsInuse());
	 		 			        	mmstats.setConnectionsCreated(stats.getConnectionsCreated());
	 		 			        	mmstats.setConnectionsDestroyed(stats.getConnectionsDestroyed());
	 		 			        	mmstats.setConnectionsWaiting(stats.getConnectionsWaiting());
	 		 			        	mmstats.setTotalConnections(stats.getTotalConnections());	 		 			        	
	 		 			        	
	 		 			        	results.add(mmstats);	 		 			        	
	 		 			        }
 		 			        }

 			            }
 			    	
 			                
 			    }
 			}
 		} catch (MetaMatrixComponentException e) {
 			throw new AdminComponentException(e);
 		} catch (ServiceException e) {
 			throw new AdminComponentException(e);
 		}
         return results;
     }    
   

    /**
     * Get monitoring information about requests.
     * @param identifier Identifier of a session or request to get information for.
     * For example "sessionID" or "sessionID.requestID".   
     * <p>If identifier is "*", this method returns information about all requests in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.Request</code>
     * @since 4.3
     */
    public Collection getRequests(String identifier) throws AdminException  {
        return getRequests(identifier, false);
    }
    
    
    private Collection getRequests(String identifier, boolean source) throws AdminException {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        ArrayList results = null;
        try {
			Collection requests = null;
			requests = getQueryServiceProxy().getAllQueries();
			
			// get the connectorbinding names all at once, its faster than one at a time
			Set uuids = new HashSet(requests.size());
			for (Iterator iter = requests.iterator(); iter.hasNext();) {
			    RequestInfo info = (RequestInfo)iter.next();
			    uuids.add(info.getConnectorBindingUUID());
			}            
			
			Map uuidToBindingNameMap = getConnectorBindingNamesMapFromUUIDs(uuids);
			//convert results into MMRequest objects
			results = new ArrayList(requests.size());
			for (Iterator iter = requests.iterator(); iter.hasNext();) {
			    RequestInfo info = (RequestInfo)iter.next();
			    if (source ^ info.isAtomicQuery()) {
			    	continue;
			    }
			    String[] identifierParts = null;

			    MMRequest request;
			    if (info.isAtomicQuery()) {
			    	identifierParts = new String[4];
			    	identifierParts[2] = String.valueOf(info.getNodeID());
			    	identifierParts[3] = String.valueOf(info.getExecutionID());
			    } else {
			    	identifierParts = new String[2];
			    }
			    identifierParts[0] = info.getRequestID().getConnectionID();
		        identifierParts[1] = Long.toString(info.getRequestID().getExecutionID());

		        request = new MMRequest(identifierParts);
			    
			    if (identifierMatches(identifier, identifierParts)) {
			        Object bindingName = uuidToBindingNameMap.get(info.getConnectorBindingUUID());
			        request.setConnectorBindingName(bindingName!=null?(String)bindingName:null);
			        request.setCreated(info.getSubmittedTimestamp()); 
			        request.setSqlCommand(info.getCommand()); 
			        request.setProcessingDate(info.getProcessingTimestamp());
			        if (info.getTransactionId() != null) {
			            request.setTransactionID(info.getTransactionId());
			        }
		            request.setUserName(info.getUserName());
			        
			        results.add(request);
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
        return results;
    }
    
    /**
     * Get monitoring information about resources.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getResources(java.lang.String)
     * @param identifier Identifier of a resource to get information for.
     * For example "resourceName".   
     * <p>If identifier is "*", this method returns information about all resources in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.Resource</code>
     * @since 4.3
     */
    public Collection getResources(String identifier) throws AdminException {
        ArrayList results = null;

        Collection resources = null;

        try {
			resources = getConfigurationServiceProxy().getResources();
			if (resources != null) {
			    results = new ArrayList(resources.size());

			    for (Iterator itr = resources.iterator(); itr.hasNext();) {
			        SharedResource sr = (SharedResource)itr.next();
			        String name = sr.getName();
			        String[] identifierParts = new String[] {
			            name
			        };
			        if (identifierMatches(identifier, identifierParts)) {
			            MMResource resource = new MMResource(identifierParts);
			            resource.setResourceType(sr.getComponentTypeID().getName());
			            resource.setCreated(sr.getCreatedDate());
			            resource.setCreatedBy(sr.getCreatedBy());
			            resource.setLastUpdated(sr.getLastChangedDate());
			            resource.setLastUpdatedBy(sr.getLastChangedBy());
			            resource.setProperties(sr.getProperties());
			            results.add(resource);
			        }
			    }
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}

        return results;
    }

    /**
     * Get monitoring information about sessions.
     * 
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getSessions(java.lang.String)
     * @param identifier
     *            Identifier of a session to get information for. For example "sessionID".
     *            <p>
     *            If identifier is "*", this method returns information about all sessions in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.Session</code>
     * @since 4.3
     */
    public Collection<Session> getSessions(String identifier) throws AdminException  {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        ArrayList results = null;
        try {
			Collection sessions = getSessionServiceProxy().getActiveSessions();
			
			//convert results into MMSession objects
			results = new ArrayList(sessions.size());
			for (Iterator iter = sessions.iterator(); iter.hasNext();) {
			    MetaMatrixSessionInfo info = (MetaMatrixSessionInfo)iter.next();

			    String sessionID = info.getSessionID().toString();
			    String[] identifierParts = new String[] {sessionID};
			    if (identifierMatches(identifier, identifierParts)) {
			        MMSession session = new MMSession(identifierParts);

			        SessionToken st = info.getSessionToken();
			        String vdbName = trimString(info.getProductInfo(ProductInfoConstants.VIRTUAL_DB));
			        String vdbVersionString = trimString(info.getProductInfo(ProductInfoConstants.VDB_VERSION));

			        session.setUserName(info.getUserName());
			        session.setCreatedBy(info.getUserName());
			        session.setApplicationName(info.getApplicationName()); 
			        session.setCreated(new Date(info.getTimeCreated())); 
			        session.setLastUpdated(new Date(info.getTimeCreated())); 
			        session.setVDBName(vdbName); 
			        session.setVDBVersion(vdbVersionString); 
			        session.setLastPingTime(info.getLastPingTime());
			        session.setSessionState(MetaMatrixSessionState.ACTIVE);
			        session.setIPAddress(info.getClientIp());
			        session.setHostName(info.getClientHostname());
			        results.add(session);
			    }
			}
		} catch (SessionServiceException e) {
			throw new AdminProcessingException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
        return results;
    }

   

    

    /**
     * Get monitoring information about source requests.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getSourceRequests(java.lang.String)
     * @param identifier Identifier of a session or source request to get information for.
     * For example "sessionID" or "sessionID.requestID".   
     * <p>If identifier is "*", this method returns information about all source requests in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.SourceRequest</code>
     * @since 4.3
     */
    public Collection getSourceRequests(String identifier) throws AdminException  {
        return getRequests(identifier, true);

    }

    /**
     * Get monitoring information about the sytem at large.
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getSystem()
     * @return object containing information about the system.
     * @since 4.3
     */
    public SystemObject getSystem() throws AdminException  {
        MMSystem system = null;
        //get state from RuntimeStateAdminAPIHelper, etc.
        try {
			boolean isStarted = getRuntimeStateAdminAPIHelper().isSystemStarted();
			Date startTime = getRuntimeStateAdminAPIHelper().getEldestProcessStartTime();
			Configuration currentConfiguration = getConfigurationServiceProxy().getCurrentConfiguration(); 
			
			system = new MMSystem();
			system.setStartTime(startTime);
			system.setStarted(isStarted);
			
			system.setProperties(currentConfiguration.getProperties());            
			system.setCreated(currentConfiguration.getCreatedDate());
			system.setCreatedBy(currentConfiguration.getCreatedBy());
			system.setLastUpdated(currentConfiguration.getLastChangedDate());
			system.setLastUpdatedBy(currentConfiguration.getLastChangedBy());
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		}
        return system;
    }

    /**
     * Get monitoring information about VDBs (Virtual Databases).
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getVDBs(java.lang.String)
     * @param identifier Identifier of a VDB to get information for.
     * For example "vdbname".   
     * <p>If identifier is "*", this method returns information about all VDBs in the system.
     * @return a <code>Collection</code> of <code>com.metamatrix.admin.api.VDB</code>
     * @since 4.3
     */
    public Collection getVDBs(String identifier) throws AdminException  {
        if (identifier == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        List results = null;
        try {
			//get vdbs from ConfigurationService
			Collection virtualDatabases = RuntimeMetadataCatalog.getInstance().getVirtualDatabases();
			
			//convert results into MMVDB objects
			results = getVDBs(identifier, virtualDatabases);
		} catch (VirtualDatabaseException e) {
			throw new AdminProcessingException(e);
		}
        return results;
    }
    
    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getPropertyDefinitions(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public Collection getPropertyDefinitions(String identifier, String className) throws AdminException {

        if (identifier == null || className == null) {
            throwProcessingException("AdminImpl.requiredparameter", new Object[] {}); //$NON-NLS-1$
        }
        
        Collection adminObjects = getAdminObjects(identifier, className);        
        if (adminObjects == null || adminObjects.size() == 0) {
            throwProcessingException("ServerMonitoringAdminImpl.No_Objects_Found", new Object[] {identifier, className}); //$NON-NLS-1$
        }
        if (adminObjects.size() > 1) {
            throwProcessingException("ServerMonitoringAdminImpl.Multiple_Objects_Found", new Object[] {identifier, className}); //$NON-NLS-1$
        }
        AdminObject adminObject = (AdminObject) adminObjects.iterator().next();
        
        try {
			ComponentObject component = null;
			String objectIdentifier = adminObject.getIdentifier();
			Configuration config;
			
			int type = MMAdminObject.getObjectType(className);
			switch (type) {
			    case MMAdminObject.OBJECT_TYPE_SYSTEM_OBJECT:
			        return convertPropertyDefinitions(getConfigurationServiceProxy().getCurrentConfiguration());
			    
			    case MMAdminObject.OBJECT_TYPE_HOST:
			        return convertPropertyDefinitions(getHostComponent(objectIdentifier));
			    
			    case MMAdminObject.OBJECT_TYPE_PROCESS_OBJECT:
			        return convertPropertyDefinitions(getProcessComponent(objectIdentifier));
			        
			    case MMAdminObject.OBJECT_TYPE_CONNECTOR_BINDING:
			        config = getConfigurationServiceProxy().getCurrentConfiguration();
			        ConnectorBinding configBinding = config.getConnectorBinding(MMAdminObject.getNameFromIdentifier(objectIdentifier));
			        
			        component = getDeployedComponent(objectIdentifier);
			        
			        return convertPropertyDefinitions(component, configBinding.getProperties());
			        
			    case MMAdminObject.OBJECT_TYPE_SERVICE:
			        config = getConfigurationServiceProxy().getCurrentConfiguration();
			        ServiceComponentDefn svc = config.getServiceComponentDefn(MMAdminObject.getNameFromIdentifier(objectIdentifier));
			        
			        component = getDeployedComponent(objectIdentifier);
			        
			        return convertPropertyDefinitions(component, svc.getProperties());

			        
			    case MMAdminObject.OBJECT_TYPE_CONNECTOR_TYPE:
			        ComponentType componentType = getConnectorTypeComponentType(objectIdentifier);
			        return convertPropertyDefinitions(componentType, new Properties());                
			        
			    case MMAdminObject.OBJECT_TYPE_DQP:
			        config = getConfigurationServiceProxy().getCurrentConfiguration();
			        ServiceComponentDefn defn = config.getServiceComponentDefn(MMAdminObject.getNameFromIdentifier(objectIdentifier));  
			        
			        return convertPropertyDefinitions(getDQPComponent(objectIdentifier), defn.getProperties());
			        
			    case MMAdminObject.OBJECT_TYPE_RESOURCE:
			        return convertPropertyDefinitions(getResourceComponent(objectIdentifier));
			    
			    default:
			        throwProcessingException("ServerMonitoringAdminImpl.Unsupported_Admin_Object", new Object[] {className}); //$NON-NLS-1$
			}
		} catch (ConfigurationException e) {
			throw new AdminComponentException(e);
		} catch (ServiceException e) {
			throw new AdminComponentException(e);
		} 
        return Collections.EMPTY_LIST;
    }

    
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#exportLogs()
     * @since 4.3
     */
    public byte[] exportLogs() throws AdminException {
        File resultFile = null;
               
        //temp dir that will contain one zip file for each host
        String tempDirName = System.getProperty("java.io.tmpdir") + File.separator + "all_logs" + System.currentTimeMillis(); //$NON-NLS-1$//$NON-NLS-2$
        try {
            resultFile = File.createTempFile("alllogs", ".zip");  //$NON-NLS-1$ //$NON-NLS-2$
            resultFile.deleteOnExit();
            
            new File(tempDirName).mkdirs();
            
            //get list of hosts RuntimeStateAdminAPIHelper
            SystemState systemState = getRuntimeStateAdminAPIHelper().getSystemState();
            Collection hostDatas = systemState.getHosts();
            
            //for each host
            for (Iterator iter = hostDatas.iterator(); iter.hasNext();) {
                HostData hostData = (HostData) iter.next();
                String hostName = hostData.getName();
                
                //pick one process under the host, and use it to get the logs for that host
                Collection processDatas = hostData.getProcesses();
                Iterator iter2 = processDatas.iterator(); 
                if (iter2.hasNext()) {
                    ProcessData processData = (ProcessData) iter2.next();
                    
                    try {
                        byte[] logBytes = getRuntimeStateAdminAPIHelper().exportLogs(processData.getHostName(), processData.getName());
                        FileUtils.convertByteArrayToFile(logBytes, tempDirName, hostName+".zip");//$NON-NLS-1$
                    } catch (MetaMatrixComponentException e) {
                        //do nothing: sometimes when the process is just starting the RMI stub
                        //for SocketVMController is not initialized yet
                    }                   
                }                   
            }
            
            //add zip files for each host to the result zip file
            ZipFileUtil.addAll(resultFile, tempDirName);
            
            
            return new FileUtil(resultFile.getAbsolutePath()).readBytes();
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        } catch(IOException e) { 		
        	throw new AdminComponentException(e);
        } finally {
            resultFile.delete();
            FileUtils.removeDirectoryAndChildren(new File(tempDirName));
        }
    }

    /**
     * @return Return a trimmed, not null version of the specified string.
     * @since 4.3
     */
    protected static String trimString(String string) {
        if (string != null) {
            return string.trim();
        } 
        
        return ""; //$NON-NLS-1$
    }
    
    
    private ComponentObject getHostComponent(String identifier) throws ConfigurationException {
        Collection hosts = getConfigurationServiceProxy().getHosts();
        for (Iterator iter = hosts.iterator(); iter.hasNext(); ) {
            Host host = (Host)iter.next();
            if (identifier.equalsIgnoreCase(host.getName())) {
                return host;
            }
        }
        return null;
    }

    private ComponentObject getProcessComponent(String identifier) throws ConfigurationException {
        Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
        Collection defns = config.getVMComponentDefns();
        for (Iterator iter = defns.iterator(); iter.hasNext(); ) {
            VMComponentDefn defn = (VMComponentDefn)iter.next();
            String[] identifierParts = new String[] {defn.getHostID().getName(), defn.getName()};                
            if (identifierMatches(identifier, identifierParts)) {
                return defn;
            }
        }
        
        return null;
    }
    
    
    
    
    
    private ComponentType getConnectorTypeComponentType(String identifier) throws ConfigurationException {
        Collection types = getConfigurationServiceProxy().getAllComponentTypes(false);
        for (Iterator iter = types.iterator(); iter.hasNext();) {
            ComponentType componentType = (ComponentType) iter.next();
            
            if (componentType.getComponentTypeCode() == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE &&
                            identifier.equalsIgnoreCase(componentType.getName())) {
                
                return componentType;
            }
        }     
        return null;
    }
    
    private ComponentObject getDQPComponent(String identifier) throws ConfigurationException {
        Configuration config = getConfigurationServiceProxy().getCurrentConfiguration();
        Collection components = config.getDeployedComponents();
        for (Iterator iter = components.iterator(); iter.hasNext(); ) {
            BasicDeployedComponent bdc = (BasicDeployedComponent)iter.next();
            
            String[] identifierParts = new String[] {
                bdc.getHostID().getName(), bdc.getVMComponentDefnID().getName(), bdc.getName()
            };
            if (QUERY_SERVICE.equals(bdc.getComponentTypeID().getName()) && 
                            identifierMatches(identifier, identifierParts)) {
                return bdc;
            }
        }
        return null;
    }
    
    private ComponentObject getResourceComponent(String identifier) throws ConfigurationException {
        Collection resources = getConfigurationServiceProxy().getResources();
        for (Iterator iter = resources.iterator(); iter.hasNext(); ) {
            
            SharedResource sr = (SharedResource)iter.next();
            if (identifier.equalsIgnoreCase(sr.getName())) {
                return sr;
            }
        }
        return null;
    }

    @Override
    public Collection<Transaction> getTransactions()
    		throws AdminException {
    	return getQueryServiceProxy().getTransactions();
    }
    
}