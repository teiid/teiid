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

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Cache;
import org.teiid.adminapi.ConnectionPool;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.ConnectorType;
import org.teiid.adminapi.ExtensionModule;
import org.teiid.adminapi.MonitoringAdmin;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.QueueWorkerPool;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl;
import com.metamatrix.server.serverapi.RequestInfo;


/** 
 * DQP implementation of the Monitoring API
 * @since 4.3
 */
public class DQPMonitoringAdminImpl extends BaseAdmin implements MonitoringAdmin {

    public DQPMonitoringAdminImpl(EmbeddedConnectionFactoryImpl manager) {
        super(manager);
    }

    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getConnectorTypes(java.lang.String)
     * @since 4.3
     */
    public Collection<ConnectorType> getConnectorTypes(String identifier) 
        throws AdminException {

        if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        return super.getConnectorTypes(identifier);
    }

    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getVDBs(java.lang.String)
     * @since 4.3
     */
    public Collection<VDB> getVDBs(String identifier) 
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
     * @see org.teiid.adminapi.MonitoringAdmin#getConnectorBindings(java.lang.String)
     * @since 4.3
     */
    public Collection<ConnectorBinding> getConnectorBindings(String identifier) 
        throws AdminException {
        
        if (identifier == null) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }                
        return super.getConnectorBindings(identifier);
    }
    

    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getConnectorBindingsInVDB(java.lang.String)
     * @since 4.3
     */
    public Collection<ConnectorBinding> getConnectorBindingsInVDB(String identifier)  throws AdminException{
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
     * @see org.teiid.adminapi.MonitoringAdmin#getExtensionModules(java.lang.String)
     * @since 4.3
     */
    public Collection<ExtensionModule> getExtensionModules(String identifier) 
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
     * @see org.teiid.adminapi.MonitoringAdmin#getQueueWorkerPools(java.lang.String)
     * @since 4.3
     */
    public Collection<QueueWorkerPool> getQueueWorkerPools(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        List results = new ArrayList();
        if (matches(identifier, "dqp")) { //$NON-NLS-1$
            // First get the queue statistics for the DQP
            Collection c = getManager().getDQP().getQueueStatistics();;
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
     * @see org.teiid.adminapi.MonitoringAdmin#getCaches(java.lang.String)
     * @since 4.3
     */
    public Collection<Cache> getCaches(String identifier) 
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
     * @see org.teiid.adminapi.MonitoringAdmin#getSessions(java.lang.String)
     * @since 4.3
     */
    public Collection<Session> getSessions(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(NUMBER_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        return matchedCollection(identifier, (List)convertToAdminObjects(getClientConnections()));
    }
        
    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getRequests(java.lang.String)
     * @since 4.3
     */
    public Collection<Request> getRequests(String identifier) 
        throws AdminException {

        if (identifier == null || !identifier.matches(NUMBER_DOT_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        ArrayList requestList = new ArrayList();
        // List contains both top and atomic requests, only add the top requests
    	List<RequestInfo> requests = getManager().getDQP().getRequests();                 
        for(RequestInfo request:requests) {
        	if (request.getConnectorBindingUUID() == null) {
        		requestList.add(request);
        	}
        }
        return matchedCollection(identifier, (List)convertToAdminObjects(requestList));
    }

    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getSourceRequests(java.lang.String)
     * @since 4.3
     */
    public Collection<Request> getSourceRequests(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(NUMBER_DOT_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        ArrayList atomicRequestList = new ArrayList();
    	List<RequestInfo> requests = getManager().getDQP().getRequests();
        for (RequestInfo request:requests) {
        	if (request.getConnectorBindingUUID() != null) {
        		atomicRequestList.add(request);
        	}
        }
        return matchedCollection(identifier, (List)convertToAdminObjects(atomicRequestList));
    }
    
    /** 
     * @see org.teiid.adminapi.MonitoringAdmin#getPropertyDefinitions(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public Collection<PropertyDefinition> getConnectorTypePropertyDefinitions(String typeName) throws AdminException {
    	
        if (typeName == null || !typeName.matches(MULTIPLE_WORDS_REGEX)) {
        	throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_ct_name")); //$NON-NLS-1$                
        }

        try {
			ConnectorBindingType type = getConfigurationService().getConnectorType(typeName);
			if (type == null) {
				throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.ct_doesnot_exist", typeName)); //$NON-NLS-1$
			}
   
			return convertPropertyDefinitions(type, new Properties());
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
	public Collection<? extends ConnectionPool> getConnectionPoolStats(String identifier)
			throws AdminException {
		
		try {
			return this.getDataService().getConnectionPoolStatistics(identifier);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}

	}

	@Override
	public Collection<ProcessObject> getProcesses(String processIdentifier) throws AdminException {
		ArrayList<ProcessObject> list = new ArrayList<ProcessObject>();
		list.add(getManager().getProcess());
		return list;
	}
}
