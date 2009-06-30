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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.EmbeddedLogger;
import org.teiid.adminapi.Group;
import org.teiid.adminapi.LogConfiguration;
import org.teiid.adminapi.ScriptsContainer;
import org.teiid.adminapi.SystemObject;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;

import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.server.HostManagement;
import com.metamatrix.server.query.service.QueryServiceInterface;

/**
 * @since 4.3
 */
public class ServerAdminImpl implements ServerAdmin {

    private ServerRuntimeStateAdminImpl runtime = null;
    private ServerConfigAdminImpl config = null;
    private ServerMonitoringAdminImpl monitoring = null;
    private ServerSecurityAdminImpl security = null;
    
    private SessionServiceInterface sessionServiceProxy = null;
    private ConfigurationServiceInterface configurationServiceProxy = null;
    private MembershipServiceInterface membershipServiceProxy = null;
    private AuthorizationServiceInterface authorizationServiceProxy = null;
    private QueryServiceInterface queryServiceProxy = null;
    private ExtensionModuleManager extensionModuleManager = null;
    private RuntimeStateAdminAPIHelper runtimeStateAdminAPIHelper = null;
    
    /**
     * How often to poll for services starting/stopping
     */
    protected final static int SERVICE_WAIT_INTERVAL = 500;
    
    ClusteredRegistryState registry;
    HostManagement hostManagement;
    
    /**
     * xtor
     * 
     * @since 4.3
     */
    public ServerAdminImpl(ClusteredRegistryState registry, HostManagement hostManagement) {
    	this.registry = registry;
    	this.hostManagement = hostManagement;
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addAuthorizationProvider(java.lang.String, java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void addAuthorizationProvider(String domainprovidername,
                                         String providertypename,
                                         Properties properties) throws AdminException {
        getConfigurationAdmin().addAuthorizationProvider(domainprovidername, providertypename, properties);
    }	

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getCaches(java.lang.String)
     * @since 4.3
     */
    public Collection getCaches(String identifier) throws AdminException {
        return getMonitoringAdmin().getCaches(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getConnectorBindings(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindings(String identifier) throws AdminException {
        return getMonitoringAdmin().getConnectorBindings(identifier);
    }

    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getConnectorBindingsInVDB(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindingsInVDB(String identifier) throws AdminException {
        return getMonitoringAdmin().getConnectorBindingsInVDB(identifier);
    }
    
    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getConnectorTypes(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorTypes(String identifier) throws AdminException {
        return getMonitoringAdmin().getConnectorTypes(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getDQPs(java.lang.String)
     * @since 4.3
     */
    public Collection getDQPs(String identifier) throws AdminException {
        return getMonitoringAdmin().getDQPs(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getExtensionModules(java.lang.String)
     * @since 4.3
     */
    public Collection getExtensionModules(String identifier) throws AdminException {
        return getMonitoringAdmin().getExtensionModules(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getHosts(java.lang.String)
     * @since 4.3
     */
    public Collection getHosts(String identifier) throws AdminException {
        return getMonitoringAdmin().getHosts(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getProcesses(java.lang.String)
     * @since 4.3
     */
    public Collection getProcesses(String identifier) throws AdminException {
        return getMonitoringAdmin().getProcesses(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getQueueWorkerPools(java.lang.String)
     * @since 4.3
     */
    public Collection getQueueWorkerPools(String identifier) throws AdminException {
        return getMonitoringAdmin().getQueueWorkerPools(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getRequests(java.lang.String)
     * @since 4.3
     */
    public Collection getRequests(String identifier) throws AdminException {
        return getMonitoringAdmin().getRequests(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getResources(java.lang.String)
     * @since 4.3
     */
    public Collection getResources(String identifier) throws AdminException {
        return getMonitoringAdmin().getResources(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getSessions(java.lang.String)
     * @since 4.3
     */
    public Collection getSessions(String identifier) throws AdminException {
        return getMonitoringAdmin().getSessions(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getSourceRequests(java.lang.String)
     * @since 4.3
     */
    public Collection getSourceRequests(String identifier) throws AdminException {
        return getMonitoringAdmin().getSourceRequests(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getSystem()
     * @since 4.3
     */
    public SystemObject getSystem() throws AdminException {
        return getMonitoringAdmin().getSystem();
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#getVDBs(java.lang.String)
     * @since 4.3
     */
    public Collection getVDBs(String identifier) throws AdminException {
        return getMonitoringAdmin().getVDBs(identifier);
    }
    
    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getPropertyDefinitions(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public Collection getPropertyDefinitions(String identifier, String className) throws AdminException {
        return getMonitoringAdmin().getPropertyDefinitions(identifier, className);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerMonitoringAdmin#exportLogs()
     * @since 4.3
     */
    public byte[] exportLogs() throws AdminException {
        return getMonitoringAdmin().exportLogs();
    }
    
    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addConnectorBinding(java.lang.String,
     *      java.lang.String, java.util.Properties, AdminOptions)
     * @since 4.3
     */
    public ConnectorBinding addConnectorBinding(String connectorBindingIdentifier,
                                    String connectorTypeIdentifier,
                                    Properties props, AdminOptions options) throws AdminException {
        return getConfigurationAdmin().addConnectorBinding(connectorBindingIdentifier, connectorTypeIdentifier, props, options);
    }

    
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addConnectorBinding(java.lang.String, char[], AdminOptions)
     * @since 4.3
     */
    public ConnectorBinding addConnectorBinding(String name,
                                    char[] xmlFile, AdminOptions options) throws AdminException {
        return getConfigurationAdmin().addConnectorBinding(name, xmlFile, options);
    }

    /**
     * @throws MetaMatrixProcessingException
     * @throws MetaMatrixComponentException
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addConnectorType(java.lang.String, char[])
     * @since 4.3
     */
    public void addConnectorType(String name,
                                 char[] cdkFile) throws AdminException {
        getConfigurationAdmin().addConnectorType(name, cdkFile);
    }    

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addConnectorArchive(byte[], org.teiid.adminapi.AdminOptions)
     * @since 4.3
     */
    public void addConnectorArchive(byte[] archiveContents, AdminOptions options) throws AdminException {
        getConfigurationAdmin().addConnectorArchive(archiveContents, options);
    }    
    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addExtensionModule(java.lang.String, java.lang.String, byte[], java.lang.String, boolean)
     * @since 4.3
     */
    public void addExtensionModule(String type,
                                   String sourceName,
                                   byte[] source,
                                   String description) throws AdminException {
        getConfigurationAdmin().addExtensionModule(type, sourceName, source, description);
    }

    /**
     * @throws MetaMatrixComponentException
     * @throws MetaMatrixProcessingException
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addHost(java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void addHost(String hostName,
                        Properties properties) throws AdminException {
        getConfigurationAdmin().addHost(hostName, properties);
    }

    /**
     * @throws MetaMatrixComponentException
     * @throws MetaMatrixProcessingException
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addProcess(java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void addProcess(String processIdentifier,
                           Properties properties) throws AdminException {
        getConfigurationAdmin().addProcess(processIdentifier, properties);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#addVDB(java.lang.String, java.lang.String, byte[], char[])
     * @since 4.3
     */
    public VDB addVDB(String name,
                       byte[] vdbFile, AdminOptions options) throws AdminException {
        return getConfigurationAdmin().addVDB(name, vdbFile, options);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#changeVDBStatus(java.lang.String, java.lang.String, int)
     * @since 4.3
     */
    public void changeVDBStatus(String name,
                                String version,
                                int status) throws AdminException {
        getRuntimeAdmin().changeVDBStatus(name, version, status);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#disableHost(java.lang.String)
     * @since 4.3
     */
    public void disableHost(String identifier) throws AdminException {
        getConfigurationAdmin().disableHost(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#disableProcess(java.lang.String)
     * @since 4.3
     */
    public void disableProcess(String identifier) throws AdminException {
        getConfigurationAdmin().disableProcess(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#enableHost(java.lang.String)
     * @since 4.3
     */
    public void enableHost(String identifier) throws AdminException {
        getConfigurationAdmin().enableHost(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#enableProcess(java.lang.String)
     * @since 4.3
     */
    public void enableProcess(String identifier) throws AdminException {
        getConfigurationAdmin().enableProcess(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#getLogConfiguration()
     * @since 4.3
     */
    public LogConfiguration getLogConfiguration() throws AdminException {
        return getConfigurationAdmin().getLogConfiguration();
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#setLogConfiguration(org.teiid.adminapi.LogConfiguration)
     * @since 4.3
     */
    public void setLogConfiguration(LogConfiguration config) throws AdminException {
        getConfigurationAdmin().setLogConfiguration(config);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#setSystemProperty(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void setSystemProperty(String property,
                                  String value) throws AdminException {
        getConfigurationAdmin().setSystemProperty(property, value);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#updateSystemProperties(java.util.Properties)
     * @since 4.3
     */
    public void updateSystemProperties(Properties properties) throws AdminException {
        getConfigurationAdmin().updateSystemProperties(properties);
    }
    
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#cancelRequest(java.lang.String)
     * @since 4.3
     */
    public void cancelRequest(String identifier) throws AdminException {
        getRuntimeAdmin().cancelRequest(identifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#cancelSourceRequest(java.lang.String)
     * @since 4.3
     */
    public void cancelSourceRequest(String identifier) throws AdminException {
        getRuntimeAdmin().cancelSourceRequest(identifier);
    }

   
    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#startConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void startConnectorBinding(String connectorBindingIdentifier) throws AdminException {
        
        getRuntimeAdmin().startConnectorBinding(connectorBindingIdentifier);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#startHost(java.lang.String, boolean)
     * @since 4.3
     */
    public void startHost(String hostName, boolean waitUntilDone) throws AdminException {
        getRuntimeAdmin().startHost(hostName, waitUntilDone);
    }

    /**
     * @see com.metamatrix.admin.api.RuntimeStateAdmin#stopConnectorBinding(java.lang.String, boolean)
     * @since 4.3
     */
    public void stopConnectorBinding(String connectorBindingIdentifier, boolean stopNow) throws AdminException  {
        
        getRuntimeAdmin().stopConnectorBinding(connectorBindingIdentifier, stopNow);
    }

    /**
     * @see com.metamatrix.admin.api.RuntimeStateAdmin#stopHost(java.lang.String, boolean, boolean)
     * @since 4.3
     */
    public void stopHost(String hostName, boolean stopNow, boolean waitUntilDone) throws AdminException  {
        getRuntimeAdmin().stopHost(hostName, stopNow, waitUntilDone);
    }

    /**
     * @see com.metamatrix.admin.api.RuntimeStateAdmin#stopProcess(java.lang.String, boolean, boolean)
     * @since 4.3
     */
    public void stopProcess(String identifier, boolean stopNow, boolean waitUntilDone) throws AdminException  {
        getRuntimeAdmin().stopProcess(identifier, stopNow, waitUntilDone);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#startProcess(java.lang.String, boolean)
     * @since 4.3
     */
    public void startProcess(String identifier, boolean waitUntilDone) throws AdminException  {
        getRuntimeAdmin().startProcess(identifier, waitUntilDone);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#stopSystem()
     * @since 4.3
     */
    public void stopSystem() throws AdminException  {
        getRuntimeAdmin().stopSystem();
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#bounceSystem(boolean)
     * @since 4.3
     */
    public void bounceSystem(boolean waitUntilDone) throws AdminException  {
        getRuntimeAdmin().bounceSystem(waitUntilDone);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#synchronizeSystem(boolean)
     * @since 4.3
     */
    public void synchronizeSystem(boolean waitUntilDone) throws AdminException  {
        getRuntimeAdmin().synchronizeSystem(waitUntilDone);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerRuntimeStateAdmin#terminateSession(java.lang.String)
     * @since 4.3
     */
    public void terminateSession(String identifier) throws AdminException  {
        getRuntimeAdmin().terminateSession(identifier);
    }

    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#clearCache(java.lang.String)
     * @since 4.3
     */
    public void clearCache(String cacheIdentifier) throws AdminException {
        
        getRuntimeAdmin().clearCache(cacheIdentifier);
    }
    
    /**
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#setProperty(java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void setProperty(String identifier,
                            String className,
                            String propertyName,
                            String propertyValue) throws AdminException {
        getConfigurationAdmin().setProperty(identifier, className, propertyName, propertyValue);

    }
    
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#updateProperties(java.lang.String, java.lang.String, java.util.Properties)
     * @since 4.3
     */
    public void updateProperties(String identifier,
                                 String className,
                                 Properties properties) throws AdminException {
        getConfigurationAdmin().updateProperties(identifier, className, properties);
    }

 
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorBinding(String connectorBindingIdentifier) throws AdminException {
        getConfigurationAdmin().deleteConnectorBinding(connectorBindingIdentifier);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteConnectorType(java.lang.String)
     * @since 4.3
     */
    public void deleteConnectorType(String name) throws AdminException {
        getConfigurationAdmin().deleteConnectorType(name);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteExtensionModule(java.lang.String)
     * @since 4.3
     */
    public void deleteExtensionModule(String sourceName) throws AdminException {
        getConfigurationAdmin().deleteExtensionModule(sourceName);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteHost(java.lang.String)
     * @since 4.3
     */
    public void deleteHost(String identifier) throws AdminException {
        getConfigurationAdmin().deleteHost(identifier);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#deleteProcess(java.lang.String)
     * @since 4.3
     */
    public void deleteProcess(String identifier) throws AdminException {
        getConfigurationAdmin().deleteProcess(identifier);
    }

    private synchronized ServerRuntimeStateAdminImpl getRuntimeAdmin() {
        if (runtime == null) {
            runtime = new ServerRuntimeStateAdminImpl(this, registry);
        }
        return runtime;
    }

    private synchronized ServerConfigAdminImpl getConfigurationAdmin() {
        if (config == null) {
            config = new ServerConfigAdminImpl(this, this.registry);
        }
        return config;
    }

    private synchronized ServerMonitoringAdminImpl getMonitoringAdmin() {
        if (monitoring == null) {
            monitoring = new ServerMonitoringAdminImpl(this, this.registry);
        }
        return monitoring;
    }

    private synchronized ServerSecurityAdminImpl getSecurityAdmin() {
        if (security == null) {
            security = new ServerSecurityAdminImpl(this, this.registry);
        }
        return security;
    }
    
    protected synchronized SessionServiceInterface getSessionServiceProxy() throws ServiceException {
        if (sessionServiceProxy == null) {
            sessionServiceProxy = PlatformProxyHelper.getSessionServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        }
        return sessionServiceProxy;
    }
    
    protected synchronized ConfigurationServiceInterface getConfigurationServiceProxy() throws ServiceException {
        if (configurationServiceProxy == null) {
            configurationServiceProxy = PlatformProxyHelper.getConfigurationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        }
        return configurationServiceProxy;
    }
    
    protected synchronized MembershipServiceInterface getMembershipServiceProxy() throws ServiceException {
        if (membershipServiceProxy == null) {
            membershipServiceProxy = PlatformProxyHelper.getMembershipServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        }
        return membershipServiceProxy;
    }
    
    protected synchronized AuthorizationServiceInterface getAuthorizationServiceProxy() throws ServiceException {
        if (authorizationServiceProxy == null) {
            authorizationServiceProxy = PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        }
        return authorizationServiceProxy;
    }
    
    protected synchronized QueryServiceInterface getQueryServiceProxy() throws ServiceException {
        if (queryServiceProxy == null) {
            queryServiceProxy = PlatformProxyHelper.getQueryServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        }
        return queryServiceProxy;
    }
    
    
    protected ExtensionModuleManager getExtensionSourceManager(){
        if (extensionModuleManager == null){
            extensionModuleManager = ExtensionModuleManager.getInstance();
        }
        return extensionModuleManager;
    }
    
    protected RuntimeStateAdminAPIHelper getRuntimeStateAdminAPIHelper(){
        if (runtimeStateAdminAPIHelper == null){
            runtimeStateAdminAPIHelper = RuntimeStateAdminAPIHelper.getInstance(this.registry, this.hostManagement);
        }
        return runtimeStateAdminAPIHelper;
    }
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getRolesForUser(java.lang.String)
     * @since 4.3
     */
    public Collection getRolesForUser(String userIdentifier) throws AdminException {
        return getSecurityAdmin().getRolesForUser(userIdentifier);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getGroupsForUser(java.lang.String)
     * @since 4.3
     */
    public Collection getGroupsForUser(String userIdentifier) throws AdminException {
        return getSecurityAdmin().getGroupsForUser(userIdentifier);
    }
    
    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getGroups(java.lang.String)
     * @since 4.3
     */
    public Collection getGroups(String groupIdentifier) throws AdminException {
        return getSecurityAdmin().getGroups(groupIdentifier);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#getRolesForGroup(java.lang.String)
     * @since 4.3
     */
    public Collection getRolesForGroup(String groupIdentifier) throws AdminException {
        return getSecurityAdmin().getRolesForGroup(groupIdentifier);
    }

    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#assignBindingToModel(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void assignBindingToModel(String connectorBindingName,
                                     String vdbName,
                                     String vdbVersion,
                                     String modelName) throws AdminException {
        getConfigurationAdmin().assignBindingToModel(connectorBindingName, vdbName, vdbVersion, modelName);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deassignBindingsFromModel(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 5.0
     */
    public void deassignBindingFromModel(String connectorBindingName,
                                     String vdbName,
                                     String vdbVersion,
                                     String modelName) throws AdminException {
        getConfigurationAdmin().deassignBindingFromModel(connectorBindingName, vdbName, vdbVersion, modelName);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#assignBindingToModel(String[], java.lang.String, java.lang.String, java.lang.String)
     * @since 5.0
     */
    public void assignBindingsToModel(String[] connectorBindingNames,
                                     String vdbName,
                                     String vdbVersion,
                                     String modelName) throws AdminException {
        getConfigurationAdmin().assignBindingsToModel(connectorBindingNames, vdbName, vdbVersion, modelName);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deassignBindingsFromModel(String[], java.lang.String, java.lang.String, java.lang.String)
     * @since 5.0
     */
    public void deassignBindingsFromModel(String[] connectorBindingNames,
                                     String vdbName,
                                     String vdbVersion,
                                     String modelName) throws AdminException {
        getConfigurationAdmin().deassignBindingsFromModel(connectorBindingNames, vdbName, vdbVersion, modelName);
    }

    /**
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#importDataRoles(java.lang.String, java.lang.String, char[], org.teiid.adminapi.AdminOptions)
     */
    public String importDataRoles(String vdbName, String vdbVersion, char[] data, AdminOptions options) throws AdminException {
        return getSecurityAdmin().importDataRoles(vdbName, vdbVersion, data, options);
    }
   
    /**
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#exportDataRoles(java.lang.String, java.lang.String)
     */
    public char[] exportDataRoles(String vdbName, String vdbVersion) throws AdminException {
        return getSecurityAdmin().exportDataRoles(vdbName, vdbVersion);
    }


    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConfiguration()
     * @since 4.3
     */
    public char[] exportConfiguration() throws AdminException {
        return getConfigurationAdmin().exportConfiguration();
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#importConfiguration()
     * @since 4.3
     */
    public void importConfiguration(char[] fileData) throws AdminException {
        getConfigurationAdmin().importConfiguration(fileData);
    }    
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public char[] exportConnectorBinding(String connectorBindingIdentifier) throws AdminException {
        return getConfigurationAdmin().exportConnectorBinding(connectorBindingIdentifier);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorType(java.lang.String)
     * @since 4.3
     */
    public char[] exportConnectorType(String connectorTypeIdentifier) throws AdminException {
        return getConfigurationAdmin().exportConnectorType(connectorTypeIdentifier);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportConnectorArchive(java.lang.String)
     * @since 4.3
     */
    public byte[] exportConnectorArchive(String connectorTypeIdentifier) throws AdminException {
        return getConfigurationAdmin().exportConnectorArchive(connectorTypeIdentifier);
    }
    
    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#exportExtensionModule(java.lang.String)
     * @since 4.3
     */
    public byte[] exportExtensionModule(String sourceName) throws AdminException {
        return getConfigurationAdmin().exportExtensionModule(sourceName);
    }

    /**  
     * @see org.teiid.adminapi.ConfigurationAdmin#exportVDB(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public byte[] exportVDB(String name, String version) throws AdminException {
        return getConfigurationAdmin().exportVDB(name, version);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#assignRoleToGroup(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void assignRoleToGroup(String roleIdentifier,
                                  String groupIdentifier) throws AdminException {
        getSecurityAdmin().assignRoleToGroup(roleIdentifier, groupIdentifier);
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerSecurityAdmin#removeRoleFromGroup(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void removeRoleFromGroup(String roleIdentifier,
                                    String grouplIdentifier) throws AdminException {
        getSecurityAdmin().removeRoleFromGroup(roleIdentifier, grouplIdentifier);
    }

    /**
     * Do nothing: this is implemented in ServerAdminClientInterceptor on the client-side.
     * @see com.metamatrix.admin.api.server.ServerAdmin#close()
     * @since 4.3
     */
    public void close() {
    }
    
    

    protected void waitForServicesToStart(Collection expectedServiceNames) throws MetaMatrixComponentException { 
        //wait until runtime matches the configuration
        boolean done = false;
        while(! done) {
            done = areServicesAttempted(expectedServiceNames); 
            try {
				Thread.sleep(SERVICE_WAIT_INTERVAL);
			} catch (InterruptedException e) {
			}
        }
    }
    
    protected void waitForServicesToStop(Collection expectedServiceNames) throws MetaMatrixComponentException { 
        //wait until runtime matches the configuration
        boolean done = false;
        while(! done) {
            done = areServicesStopped(expectedServiceNames); 
            try {
				Thread.sleep(SERVICE_WAIT_INTERVAL);
			} catch (InterruptedException e) {
			}
        }
    }
    
    /**
     * Check the runtime services.  Check that all of the <code>expectedServiceNames</code>
     * are either stopped, or unknown to the runtime state 
     * @param expectedServiceNames Full-names of expected services.
     * @return
     * @since 4.3
     */
    private boolean areServicesStopped(Collection expectedServiceNames) throws MetaMatrixComponentException {
        Collection<ServiceRegistryBinding> services = getRuntimeStateAdminAPIHelper().getServices();            
        for (ServiceRegistryBinding serviceBinding:services) {
            DeployedComponent deployedComponent = serviceBinding.getDeployedComponent();
            if (expectedServiceNames.contains(deployedComponent.getID().getFullName())) {
                int state = serviceBinding.getCurrentState();
                boolean stopped = isStateStopped(state);
                if (!stopped) {
                    return false;
                } 
            }
        }
        
        return true;
    }
    
    /**
     * Check the runtime services.  Check that it contains all of the <code>expectedServiceNames</code>,
     * and that it does not contain any services that have not attempted to start. 
     * @param expectedServiceNames Full-names of expected services.
     * @return
     * @since 4.3
     */
    private boolean areServicesAttempted(Collection expectedServiceNames) throws MetaMatrixComponentException {
        
        Collection attemptedServiceNames = new ArrayList();
        Collection notAttemptedServiceNames = new ArrayList();
        Collection<ServiceRegistryBinding> services = getRuntimeStateAdminAPIHelper().getServices();            
        for (ServiceRegistryBinding serviceBinding:services) {
            DeployedComponent deployedComponent = serviceBinding.getDeployedComponent();
            int state = serviceBinding.getCurrentState();
            boolean attempted = isStateAttempted(state);
            if (attempted) {
                attemptedServiceNames.add(deployedComponent.getID().getFullName());
            } else {
                notAttemptedServiceNames.add(deployedComponent.getID().getFullName());
            }
        }
        
        if (notAttemptedServiceNames.size() > 0) {
            return false;
        }
        return attemptedServiceNames.containsAll(expectedServiceNames);
    }
    
    
    private boolean isStateStopped(int state) {
        return state != ServiceState.STATE_OPEN &&
            state != ServiceState.STATE_DATA_SOURCE_UNAVAILABLE;
    }
    

    private boolean isStateAttempted(int state) {
        return state == ServiceState.STATE_OPEN ||
            state == ServiceState.STATE_FAILED ||
            state == ServiceState.STATE_INIT_FAILED ||            
            state == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE;
    }

    /** 
     * @see com.metamatrix.admin.api.server.ServerConfigAdmin#generateMaterializationScripts(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     * @since 4.3
     */
    public ScriptsContainer generateMaterializationScripts(String vdbName,
                                                           String vdbVersion,
                                                           String metamatrixUserName,
                                                           String metamatrixUserPwd,
                                                           String materializationUserName,
                                                           String materializationUserPwd) throws AdminException {
        return getConfigurationAdmin().generateMaterializationScripts( vdbName,
                                                                       vdbVersion,
                                                                       metamatrixUserName,
                                                                       metamatrixUserPwd,
                                                                       materializationUserName,
                                                                       materializationUserPwd);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#addUDF(byte[], java.lang.String)
     */
    public void addUDF(byte[] modelFileContents,String classpath) throws AdminException {
        getConfigurationAdmin().addUDF(modelFileContents, classpath);
    }

    /** 
     * @see org.teiid.adminapi.ConfigurationAdmin#deleteUDF()
     */
    public void deleteUDF() throws AdminException {
        getConfigurationAdmin().deleteUDF();
    }
    
    @Override
    public Properties getBootstrapProperties() throws AdminException {
    	return getConfigurationAdmin().getBootstrapProperties();
    }
    
    @Override
    public byte[] getClusterKey() throws AdminException {
    	return getConfigurationAdmin().getClusterKey();
    }

	@Override
	public boolean authenticateUser(String username, char[] credentials,
			Serializable trustePayload, String applicationName)
			throws AdminException {
		return getSecurityAdmin().authenticateUser(username, credentials, trustePayload, applicationName);
	}
	
	@Override
	public List<String> getDomainNames() throws AdminException {
		return getSecurityAdmin().getDomainNames();
	}
	
	@Override
	public Collection<Group> getGroupsForDomain(String domainName)
			throws AdminException {
		return getSecurityAdmin().getGroupsForDomain(domainName);
	}
	
	@Override
	public Collection<Transaction> getTransactions()
			throws AdminException {
		return getMonitoringAdmin().getTransactions();
	}
	
	@Override
	public void terminateTransaction(String transactionId, String sessionId)
			throws AdminException {
		getRuntimeAdmin().terminateTransaction(transactionId, sessionId);
	}
	
	@Override
	public void terminateTransaction(Xid transactionId) throws AdminException {
		getRuntimeAdmin().terminateTransaction(transactionId);
	}

	@Override
	public Collection getServices(String identifier) throws AdminException {
		// TODO Auto-generated method stub
		return getMonitoringAdmin().getServices(identifier);
	}

	@Override
	public Collection getConnectionPoolStats(String identifier)
			throws AdminException {
		return getMonitoringAdmin().getConnectionPoolStats(identifier);
	}

	@Override
	public void extensionModuleModified(String name) throws AdminException {
	}

	@Override
	public void restart() throws AdminException {
	}

	@Override
	public void setLogListener(EmbeddedLogger listener) throws AdminException {
	}

	@Override
	public void shutdown(int millisToWait) throws AdminException {
	}

}