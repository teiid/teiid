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

package com.metamatrix.dqp.embedded.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.admin.api.embedded.EmbeddedMonitoringAdmin;
import com.metamatrix.admin.api.exception.AdminComponentException;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.admin.api.objects.AdminObject;
import com.metamatrix.admin.api.objects.ConnectorBinding;
import com.metamatrix.admin.api.objects.SystemObject;
import com.metamatrix.admin.api.objects.Transaction;
import com.metamatrix.admin.objects.MMAdminObject;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl;
import com.metamatrix.server.serverapi.RequestInfo;


/** 
 * DQP implementation of the Monitoring API
 * @since 4.3
 */
public class DQPMonitoringAdminImpl extends BaseAdmin implements EmbeddedMonitoringAdmin {

    public DQPMonitoringAdminImpl(EmbeddedConnectionFactoryImpl manager) {
        super(manager);
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getConnectorTypes(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorTypes(String identifier) 
        throws AdminException {

        if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        return super.getConnectorTypes(identifier);
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getVDBs(java.lang.String)
     * @since 4.3
     */
    public Collection getVDBs(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(VDB_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        // if . and * not specified, add a STAR at the end to compensate for the
        // version number matching.
        if (identifier.indexOf(DOT) == -1 && identifier.indexOf(STAR) == -1) {
            identifier = identifier +DOT+STAR;
        }
        
        try {
            List<VDBArchive> vdbs = getVDBService().getAvailableVDBs();
            List matchedVdbs = new ArrayList();
            for (VDBArchive vdb:vdbs) {
                if (matches(identifier, vdb.getName()+"."+vdb.getVersion())) { //$NON-NLS-1$
                    matchedVdbs.add(vdb);
                }
            }                        
            return (List)convertToAdminObjects(matchedVdbs);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getConnectorBindings(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindings(String identifier) 
        throws AdminException {
        
        if (identifier == null) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }                
        return super.getConnectorBindings(identifier);
    }
    

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getConnectorBindingsInVDB(java.lang.String)
     * @since 4.3
     */
    public Collection getConnectorBindingsInVDB(String identifier)  throws AdminException{
        Collection<VDBArchive> vdbs = null;
        HashMap bindings = new HashMap();

        if (identifier == null || !identifier.matches(VDB_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        // if . and * not specified, add a STAR at the end to compensate for the
        // version number matching.
        if (identifier.indexOf(DOT) == -1 && identifier.indexOf(STAR) == -1) {
            identifier = identifier + STAR;
        }
                
        try {
            // first get all the VDBS in the system and loop though each of them
            vdbs = getVDBService().getAvailableVDBs();                    
            for (VDBArchive vdb:vdbs) {
                if (matches(identifier, vdb.getName()+"."+vdb.getVersion())) { //$NON-NLS-1$
                    Map connectorBindings = vdb.getConfigurationDef().getConnectorBindings();
                    bindings.putAll(connectorBindings);
                }
            }
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }      
        return (List)convertToAdminObjects(bindings.values());
    }    
    
    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getExtensionModules(java.lang.String)
     * @since 4.3
     */
    public Collection getExtensionModules(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(WORD_AND_DOT_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
                
        try {
            List extModules = getConfigurationService().getExtensionModules();
            extModules = (List)convertToAdminObjects(extModules);
            return matchedCollection(identifier, extModules);            
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getQueueWorkerPools(java.lang.String)
     * @since 4.3
     */
    public Collection getQueueWorkerPools(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        List results = new ArrayList();
        if (matches(identifier, "dqp")) { //$NON-NLS-1$
            // First get the queue statistics for the DQP
            Collection c = manager.getDQP().getQueueStatistics();;
            if (c != null && !c.isEmpty()) {
                results.addAll(c);
            }
        }
        
        try {
            // Now get for all the connector bindings
            Collection bindings = super.getConnectorBindings(identifier);
            for (Iterator i = bindings.iterator(); i.hasNext();) {
                ConnectorBinding binding = (ConnectorBinding)i.next();
                Collection c = getDataService().getConnectorBindingStatistics(binding.getName());
                if (c != null && !c.isEmpty()) {
                    results.addAll(c);
                }                
            }
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
                
        if (!results.isEmpty()) {
            return (List)convertToAdminObjects(results);
        }
        return Collections.EMPTY_LIST;
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getCaches(java.lang.String)
     * @since 4.3
     */
    public Collection getCaches(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(SINGLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        List cacheList = new ArrayList();
        for (int i =0; i < cacheTypes.length; i++) {
            if (matches(identifier, cacheTypes[i])) {
                cacheList.add(cacheTypes[i]);
            }            
        }
        return cacheList;
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getSessions(java.lang.String)
     * @since 4.3
     */
    public Collection getSessions(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(NUMBER_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        ArrayList goodConections = new ArrayList();
        Set<ServerConnection> connections = getClientConnections();
        for (Iterator i = connections.iterator(); i.hasNext();) {
        	ServerConnection connection = (ServerConnection)i.next();
            if (connection.isOpen()) {
                goodConections.add(connection);
            }
        }
        return matchedCollection(identifier, (List)convertToAdminObjects(goodConections));
    }
        
    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getRequests(java.lang.String)
     * @since 4.3
     */
    public Collection getRequests(String identifier) 
        throws AdminException {

        if (identifier == null || !identifier.matches(NUMBER_DOT_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        ArrayList requestList = new ArrayList();
        // List contains both top and atomic requests, only add the top requests
    	List<RequestInfo> requests = manager.getDQP().getRequests();                 
        for(RequestInfo request:requests) {
        	if (request.getConnectorBindingUUID() == null) {
        		requestList.add(request);
        	}
        }
        return matchedCollection(identifier, (List)convertToAdminObjects(requestList));
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getSourceRequests(java.lang.String)
     * @since 4.3
     */
    public Collection getSourceRequests(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(NUMBER_DOT_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        ArrayList atomicRequestList = new ArrayList();
    	List<RequestInfo> requests = manager.getDQP().getRequests();
        for (RequestInfo request:requests) {
        	if (request.getConnectorBindingUUID() != null) {
        		atomicRequestList.add(request);
        	}
        }
        return matchedCollection(identifier, (List)convertToAdminObjects(atomicRequestList));
    }


    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getSystem()
     * @since 4.3
     */
    public SystemObject getSystem(){
        return super.getSystem();
    }
    
    /** 
     * @see com.metamatrix.admin.api.core.CoreMonitoringAdmin#getPropertyDefinitions(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public Collection getPropertyDefinitions(String identifier, String className) throws AdminException {
        Collection adminObjects = getAdminObjects(identifier, className);        
        if (adminObjects == null || adminObjects.size() == 0) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.No_Objects_Found", identifier, className)); //$NON-NLS-1$
        }
        if (adminObjects.size() > 1) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Multiple_Objects_Found", identifier, className)); //$NON-NLS-1$
        }
        AdminObject adminObject = (AdminObject) adminObjects.iterator().next();
        
        
        try {            
            String objectIdentifier = adminObject.getIdentifier();
            ComponentType ctype = null; 
            
            int type = MMAdminObject.getObjectType(className);
            switch(type) {
            
                case MMAdminObject.OBJECT_TYPE_SYSTEM_OBJECT:
                    Properties properties = manager.getProperties();
                    return convertPropertyDefinitions(properties);
                    
                case MMAdminObject.OBJECT_TYPE_CONNECTOR_BINDING:
                    String ctypeName = ((ConnectorBinding) adminObject).getConnectorTypeName();
                    ctype = getConfigurationService().getSystemConfiguration().getComponentType(ctypeName);
                    
                    //reload the properties so they are not stale
                    ConnectorBinding binding = (ConnectorBinding) getConnectorBindings(objectIdentifier).iterator().next();
                    
                    return convertPropertyDefinitions(ctype, binding.getProperties());
    
                case MMAdminObject.OBJECT_TYPE_CONNECTOR_TYPE:
                    ctype = getConfigurationService().getSystemConfiguration().getComponentType(adminObject.getName());
                
                    return convertPropertyDefinitions(ctype, new Properties());
                
                default:
                    throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Unsupported_Object_Class", className)); //$NON-NLS-1$
                        
            }
            
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }

    @Override
    public Collection<Transaction> getTransactions()
    		throws AdminException {
    	TransactionService ts = getTransactionService();
    	if (ts == null) {
    		return Collections.emptyList();
    	}
    	return ts.getTransactions();
    }

	@Override
	public Collection getConnectionPoolStats(String identifier)
			throws AdminException {
		
		try {
			return this.getDataService().getConnectionPoolStatistics(identifier);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}

	}
    
    
    
}
