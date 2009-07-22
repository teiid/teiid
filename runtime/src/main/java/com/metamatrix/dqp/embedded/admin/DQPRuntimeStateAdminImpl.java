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
import java.util.Iterator;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Cache;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.EmbeddedLogger;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.RuntimeStateAdmin;

import com.metamatrix.admin.objects.MMRequest;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.jdbc.EmbeddedConnectionFactoryImpl;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;


/** 
 * @since 4.3
 */
public class DQPRuntimeStateAdminImpl  extends BaseAdmin implements RuntimeStateAdmin {

    public DQPRuntimeStateAdminImpl(EmbeddedConnectionFactoryImpl manager) {
        super(manager);
    }

    /** 
     * @see com.metamatrix.admin.api.embedded.EmbeddedRuntimeStateAdmin#stop(int)
     * @since 4.3
     */
    public void shutdown(int millisToWait) throws AdminException {        
        // TODO: rreddy need to implement the time to wait.
        // First terminate all the sessions to the DQP currently have
        terminateSession(AdminObject.WILDCARD);
        
        getManager().shutdown(false);
    }

    /** 
     * @see com.metamatrix.admin.api.embedded.EmbeddedRuntimeStateAdmin#restart()
     * @since 4.3
     */
    public void restart() throws AdminException {
        // First terminate all the sessions to the DQP currently have
        terminateSession(AdminObject.WILDCARD);
        
        // Now shutdown the DQP, it will automatically start next timea new connection is 
        // requested.
        getManager().shutdown(true);                        
    }

    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#startConnectorBinding(java.lang.String)
     * @since 4.3
     */
    public void startConnectorBinding(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        AdminException exceptionWrapper = null;
        // Get all matching connector bindings
        Collection bindings = getConnectorBindings(identifier);
        if (bindings != null && !bindings.isEmpty()) {
            for (Iterator i = bindings.iterator(); i.hasNext();) {
                try {
                    AdminObject binding = (AdminObject)i.next();
                    getDataService().startConnectorBinding(binding.getName());
                } catch (ApplicationLifecycleException e) {
                    exceptionWrapper = accumulateSystemException(exceptionWrapper, e);
                } catch (MetaMatrixComponentException e) {
                    exceptionWrapper = accumulateSystemException(exceptionWrapper, e);
                }
            }
        }
        else {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Connector_binding_does_not_exists", new Object[] {identifier})); //$NON-NLS-1$            
        }

        // If any errors occurred then thow the exception.
        if (exceptionWrapper != null) {
            throw exceptionWrapper;
        }
    }

    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#stopConnectorBinding(java.lang.String, boolean)
     * @since 4.3
     */
    public void stopConnectorBinding(String identifier, boolean stopNow) 
        throws AdminException {
        // TODO: need to implement "now" part
        if (identifier == null || !identifier.matches(MULTIPLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        AdminException exceptionWrapper = null;
        // Get all matching connector bindings        
        Collection bindings = getConnectorBindings(identifier);
        if (bindings != null && !bindings.isEmpty()) {
            for (Iterator i = bindings.iterator(); i.hasNext();) {
                try {
                    AdminObject binding = (AdminObject)i.next();
                    getDataService().stopConnectorBinding(binding.getName());
                } catch (ApplicationLifecycleException e) {
                    exceptionWrapper = accumulateSystemException(exceptionWrapper, e);
                } catch (MetaMatrixComponentException e) {
                    exceptionWrapper = accumulateSystemException(exceptionWrapper, e);
                }
            }
        }
        else {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Connector_binding_does_not_exists", new Object[] {identifier})); //$NON-NLS-1$            
        }

        // If any errors occurred then thow the exception.
        if (exceptionWrapper != null) {
            throw exceptionWrapper;
        }
    }

    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#clearCache(java.lang.String)
     * @since 4.3
     */
    public void clearCache(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches(SINGLE_WORD_WILDCARD_REGEX)) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        boolean processed = false;
        
        for (int i = 0; i < cacheTypes.length; i++) {
            if (matches(identifier, cacheTypes[i])) {
                if(cacheTypes[i].equals(Cache.CODE_TABLE_CACHE)) {
                    processed = true;
                    manager.getDQP().clearCodeTableCache();
                } else if(cacheTypes[i].equals(Cache.PREPARED_PLAN_CACHE)) {
                    processed = true;
                    manager.getDQP().clearPlanCache();
                } else if(cacheTypes[i].equals( Cache.QUERY_SERVICE_RESULT_SET_CACHE)) {
                    processed = true;
                    manager.getDQP().clearResultSetCache();
                } else if (cacheTypes[i].equals(Cache.CONNECTOR_RESULT_SET_CACHE)) {
                    processed = true;
                    try {
                        // Now get for all the connector bindings
                        Collection bindings = super.getConnectorBindings("*"); //$NON-NLS-1$
                        for (Iterator iter = bindings.iterator(); iter.hasNext();) {
                            ConnectorBinding binding = (ConnectorBinding)iter.next();
                            getDataService().clearConnectorBindingCache(binding.getName());
                        }
                    } catch (MetaMatrixComponentException e) {
                    	throw new AdminComponentException(e);
                    }                                        
                } 
            }
        }
        
        if (!processed) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.invalid_request", new Object[] {identifier})); //$NON-NLS-1$
        }        
    }
    
    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#terminateSession(java.lang.String)
     * @since 4.3
     */
    public void terminateSession(String identifier) 
        throws AdminException {
        
        Collection<MetaMatrixSessionInfo> sessions = getClientConnections();
        ArrayList<MetaMatrixSessionInfo> matchedConnections = new ArrayList<MetaMatrixSessionInfo>();
        
        for (MetaMatrixSessionInfo info:sessions) {
            String id = info.getSessionID().toString();
            if (matches(identifier, id)) {
                matchedConnections.add(info);
            }
        }

        // Double iteration because to avoid concurrent modification of underlying map.
        for (MetaMatrixSessionInfo info: matchedConnections) {
        	try {
				this.manager.getDQP().terminateConnection(info.getSessionID().toString());
			} catch (MetaMatrixComponentException e) {
				throw new AdminComponentException(e);
			}
        }
    }
    
    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#cancelRequest(java.lang.String)
     * @since 4.3
     */
    public void cancelRequest(String identifier) 
        throws AdminException {

        if (identifier == null || !identifier.matches("\\d+\\" + Request.DELIMITER + "\\d+")) { //$NON-NLS-1$ //$NON-NLS-2$
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
                
        String[] identifierParts = MMRequest.buildIdentifierArray(identifier);
        String connId = identifierParts[0];
        long requestId = Long.parseLong(identifierParts[1]);
        
        // get the client connection
        RequestID id = new RequestID(connId, requestId);    
        
        try {
			this.manager.getDQP().cancelRequest(id);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
    }

    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#cancelSourceRequest(java.lang.String)
     * @since 4.3
     */
    public void cancelSourceRequest(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches("\\d+\\" + Request.DELIMITER + "\\d+\\" + Request.DELIMITER + "\\d+" + Request.DELIMITER + "\\d+")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        String[] identifierParts = MMRequest.buildIdentifierArray(identifier);

        String connId = identifierParts[0];
        long requestId = Long.parseLong(identifierParts[1]);
        int nodeId = Integer.parseInt(identifierParts[2]);
        int executionId = Integer.parseInt(identifierParts[3]);
        AtomicRequestID id = new AtomicRequestID(new RequestID(connId, requestId), nodeId, executionId);

        try {
            this.manager.getDQP().cancelAtomicRequest(id);
		} catch (MetaMatrixComponentException e) {
			throw new AdminComponentException(e);
		}
    }

    /** 
     * @see org.teiid.adminapi.RuntimeStateAdmin#changeVDBStatus(java.lang.String, java.lang.String, int)
     * @since 4.3
     */
    public void changeVDBStatus(String name, String version, int status) 
        throws AdminException {
        try {
            
            if (name == null || version == null || !name.matches(SINGLE_WORD_REGEX)) {
                throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_vdb_name")); //$NON-NLS-1$
            }
            
            // Now change the VDB status it self
            this.getVDBService().changeVDBStatus(name, version, status);
            
            // If the VDB is modified and if its status changed to DELETED, then
            // we can remove all the connector bindings associated with this VDB
            // the above delete will also remove them 
        } catch (ApplicationLifecycleException e) {
        	throw new AdminComponentException(e);
        } catch (MetaMatrixComponentException e) {
        	throw new AdminComponentException(e);
        }
    }
    
    
    /** 
     * @see com.metamatrix.admin.api.embedded.EmbeddedRuntimeStateAdmin#setLogListener(java.lang.Object)
     * @since 4.3
     */
    public void setLogListener(EmbeddedLogger listener) 
        throws AdminException {
        if(listener != null) {
        	LogManager.setLogListener(new DQPLogListener(listener));
        }
        else {
            throw new AdminProcessingException("Admin_invalid_log_listener"); //$NON-NLS-1$
        }
    }
    
    @Override
    public void terminateTransaction(String transactionId, String sessionId)
    		throws AdminException {
    	TransactionService ts = getTransactionService();
    	if (ts != null) {
    		ts.terminateTransaction(transactionId, sessionId);
    	}
    }
    
    @Override
    public void terminateTransaction(Xid transactionId) throws AdminException {
    	TransactionService ts = getTransactionService();
    	if (ts != null) {
    		ts.terminateTransaction(transactionId);
    	}
    }
    
}
