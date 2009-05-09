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

package com.metamatrix.admin.api.server;

import java.util.Collection;
import java.util.Properties;

import com.metamatrix.admin.RolesAllowed;
import com.metamatrix.admin.api.core.CoreConfigAdmin;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.objects.ConnectorBinding;
import com.metamatrix.admin.api.objects.ScriptsContainer;
import com.metamatrix.admin.api.objects.VDB;


/**
 * Interface that exposes MetaMatrix server configuration for administration.
 * <p>
 * Clients should <i>not</i> code directly to this interface but should instead use {@link ServerAdmin}.
 * </p>
 *
 * @since 4.3
 */
@RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
public interface ServerConfigAdmin extends
                                  CoreConfigAdmin {

    /**
     * Generate the scripts necessary for loading or refreshing a VDB with Materialized Views.
     * <p>
     * This method requires that the VDB version exists in the system and that materialization
     * model(s) have connector bindings bound to them.
     *  
     * @param vdbName the name of the VDB containing that materialized views.
     * @param vdbVersion the version of the VDB.
     * @param metamatrixUserName the user that will be used to run the load transformation in
     * MetaMatrix (Load).
     * @param metamatrixUserPwd the MetaMatrix user's password.
     * @param materializationUserName the user that will be used to log in directly to the
     * Materialization database and run the DDL (Create, Truncate, Swap). 
     * @param materializationUserPwd the materialization user's password.
     * @return The container of scripts that can be saved to a directory.
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    ScriptsContainer generateMaterializationScripts(String vdbName, String vdbVersion, 
                                                    String metamatrixUserName, String metamatrixUserPwd, 
                                                    String materializationUserName, String materializationUserPwd) throws AdminException;
    
    /**
     * Add a Host with Properties to MetaMatrix System
     *
     * @param hostIdentifier
     *            Name of {@link com.metamatrix.admin.api.objects.Host Host} to add
     * @param properties
     *            name,value
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void addHost(String hostIdentifier,
                 Properties properties) throws AdminException;

    /**
     * Enable Host in Configuration
     *
     * @param hostIdentifier
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void enableHost(String hostIdentifier) throws AdminException;

    /**
     * Disable Host in Configuration
     *
     * @param identifier
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void disableHost(String identifier) throws AdminException;

    /**
     * Delete Host in Configuration
     * <p>
     * Note that this method may invalidate your connection to the Admin API, if you are connected to the host you are deleting.  
     * You may receive an exception on the next call to the API, and then it will recover.
     * As a workaround, you can close your connection and obtain a new connection.
     * @param identifier
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void deleteHost(String identifier) throws AdminException;

    /**
     * Delete Process in Configuration
     *
     * <p>
     * Note that this method may invalidate your connection to the Admin API, if you are connected to the process you are deleting.  
     * You may receive an exception on the next call to the API, and then it will recover.
     * As a workaround, you can close your connection and obtain a new connection.
     * @param identifier
     *            Process Identifer
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void deleteProcess(String identifier) throws AdminException;

    /**
     * Enable Process in Configuration
     *
     * @param identifier
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void enableProcess(String identifier) throws AdminException;

    /**
     * Disable Process in Configuration
     *
     * @param identifier
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void disableProcess(String identifier) throws AdminException;

    /**
     * Add Process to specified host in the processIdentifier
     *
     * @param processIdentifier
     *            Process Identifiers is Host Name.Process Name e.g. "myhost.MetaMatrixProcess"
     * @param properties
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void addProcess(String processIdentifier,
                    Properties properties) throws AdminException;

    
    
    /**
     * Import the Configuration from a character array in XML format.
     * NOTE: This imports the specified data to the "Next Startup" configuration. 
     * The server must be restarted for the new configuration to take effect.  
     * 
     * @param fileData Contents of configuration file, in XML format, as exported by {@link #exportConfiguration()}
     * @throws AdminException
     *             if there's a system error.
     * @since 4.3
     */
    void importConfiguration(char[] fileData) throws AdminException;

    /**
     * Deassign a {@link ConnectorBinding} from a {@link VDB}'s Model
     *
     * @param connectorBindingName
     *            Name of the ConnectorBinding
     * @param vdbName
     *            Name of the VDB
     * @param vdbVersion
     *            Version of the VDB
     * @param modelName
     *            Name of the Model to unmap Connector Binding
     * @throws AdminException
     *             if there's a system error or if there's a user input error.
     * @since 4.3
     */
    void deassignBindingFromModel(String connectorBindingName,
                                   String vdbName,
                                   String vdbVersion,
                                   String modelName) throws AdminException;

    /**
     * Assign {@link ConnectorBinding}s to a {@link VDB}'s Model.  If the supplied model does not 
     * support MultiSource bindings, then only the first binding in the supplied array is assigned and
     * the remainder are ignored.
     *
     * @param connectorBindingNames
     *            Names of the ConnectorBindings
     * @param vdbName
     *            Name of the VDB
     * @param vdbVersion
     *            Version of the VDB
     * @param modelName
     *            Name of the Model to map Connector Bindings
     * @throws AdminException
     *             if there's a system error or if there's a user input error.
     * @since 4.3
     */
    void assignBindingsToModel(String[] connectorBindingNames,
                              String vdbName,
                              String vdbVersion,
                              String modelName) throws AdminException;

    /**
     * Deassign {@link ConnectorBinding}s from a {@link VDB}'s Model.  Any of the supplied array of
     * bindings are deassigned from the supplied model.
     *
     * @param connectorBindingNames
     *            Names of the ConnectorBindings
     * @param vdbName
     *            Name of the VDB
     * @param vdbVersion
     *            Version of the VDB
     * @param modelName
     *            Name of the Model to unmap Connector Bindings
     * @throws AdminException
     *             if there's a system error or if there's a user input error.
     * @since 4.3
     */
    void deassignBindingsFromModel(String[] connectorBindingNames,
                                   String vdbName,
                                   String vdbVersion,
                                   String modelName) throws AdminException;
    
    
    
    /** 
     * @param domainprovidername is the name to be assigned to the newly created {@link AuthenticationProvider}
     * @param providertypename is the type of provider to create.  
     * There are 3 installed provider types and they are: 
     * <ul>
     *      <li> <code>File Membership Domain Provider</code>
     *      <li> <code>LDAP Membership Domain Provider</code> 
     *      <li><code>Custom Membership Domain Provider</code>
     * </ul>
     * @param properties are the settings specified by the providertype to be used
     * @throws AdminException
     *             if there's a system error.
     * @since 5.5.2
     */
    void addAuthorizationProvider(String domainprovidername, String providertypename, Properties properties) throws AdminException;
    
    /**
     * Return the bootstrap properties used to configure initialize the system.
     * @return
     * @throws AdminException
     * @since 6.0.0
     */
    Properties getBootstrapProperties() throws AdminException;
    
    /**
     * Retrieves the cluster key that authenticates and secures intra-cluster communication.
     * @return the cluster key or null if encryption is disabled
     * @throws AdminException
     * @since 6.0.0
     */
    byte[] getClusterKey() throws AdminException;
    
    
    /**
     * Get the Services that correspond to the specified identifier pattern.  
     * These services represent the unique instance to be configured and instantiated
     * when the server starts up.   To monitor the current state of a running service,
     * call {@link ServerMonitoringAdmin#getServices(String)} 
     *
     * @param resourceIdentifier the unique identifier for for a {@link com.metamatrix.admin.api.objects.Service Service}
     * in the system or "{@link com.metamatrix.admin.api.objects.AdminObject#WILDCARD WILDCARD}"
     * if all Services are desired.
     * 
     * NOTE: The {@link Service Service} is used for both monitoring and configuration.   For a Service returned as part of configuration,
     * the State will not be applicable.   To find the state of a current running instance, call {@link ServerMonitoringAdmin#getServices(String)} 
     * 
     * @return Collection of {@link com.metamatrix.admin.api.objects.Service Service}
     * @throws AdminException if there's a system error.
     * @since 6.1
     */   
    Collection getServicesToConfigure(String resourceIdentfier) throws AdminException;
    
    /**
     * Get the ConnectorBindings that correspond to the specified identifir pattern.
     * These connectorbindings represent the unique instance to be configured and instantiated
     * when the server starts up.   To monitor the current state of a running connectorbinding,
     * call {@link ServerMonitoringAdmin#getConnectorBindings(String)} 
     *
     * @param resourceIdentifier the unique identifier for for a {@link com.metamatrix.admin.api.objects.ConnectorBinding ConnectorBinding}
     * in the system or "{@link com.metamatrix.admin.api.objects.AdminObject#WILDCARD WILDCARD}"
     * if all Services are desired.
     * 
     * NOTE: The {@link ConnectorBinding ConnectorBinding} is used for both monitoring and configuration.   For a Service returned as part of configuration,
     * the State will not be applicable.   To find the state of a current running instance, call {@link ServerMonitoringAdmin#getConnectorBinding(String)} 
     * 
     * @return Collection of {@link com.metamatrix.admin.api.objects.ConnectorBinding ConnectorBinding}
     * @throws AdminException if there's a system error.
     * @since 6.1
     */   
    Collection getConnectorBindingsToConfigure(String resourceIdentfier) throws AdminException;


}
