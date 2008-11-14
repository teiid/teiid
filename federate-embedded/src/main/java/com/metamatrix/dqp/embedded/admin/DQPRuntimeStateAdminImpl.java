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

package com.metamatrix.dqp.embedded.admin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.metamatrix.admin.api.embedded.EmbeddedLogger;
import com.metamatrix.admin.api.embedded.EmbeddedRuntimeStateAdmin;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.admin.api.objects.AdminObject;
import com.metamatrix.admin.api.objects.Cache;
import com.metamatrix.admin.api.objects.ConnectorBinding;
import com.metamatrix.admin.api.objects.Request;
import com.metamatrix.admin.objects.MMRequest;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.dqp.embedded.DQPEmbeddedManager;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.embedded.EmbeddedConfigUtil;
import com.metamatrix.dqp.message.RequestID;


/** 
 * @since 4.3
 */
public class DQPRuntimeStateAdminImpl  extends BaseAdmin implements EmbeddedRuntimeStateAdmin {

    public DQPRuntimeStateAdminImpl(DQPEmbeddedManager manager) {
        super(manager);
    }

    /** 
     * @see com.metamatrix.admin.api.embedded.EmbeddedRuntimeStateAdmin#stop(int)
     * @since 4.3
     */
    public void stop(int millisToWait) throws AdminException {        
        // TODO: rreddy need to implement the time to wait.
        try {
            // First terminate all the sessions to the DQP currently have
            terminateSession(AdminObject.WILDCARD);
            
            getManager().shutdown();
        } catch (ApplicationLifecycleException e) {
        	throw createSystemException(e);
        }
    }

    /** 
     * @see com.metamatrix.admin.api.embedded.EmbeddedRuntimeStateAdmin#restart()
     * @since 4.3
     */
    public void restart() throws AdminException {
        try {
            // First terminate all the sessions to the DQP currently have
            terminateSession(AdminObject.WILDCARD);
            
            // Now shutdown the DQP, it will automatically start next timea new connection is 
            // requested.
            getManager().shutdown();                        
        } catch (ApplicationLifecycleException e) {
        	throw createSystemException(e);
        } 
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreRuntimeStateAdmin#startConnectorBinding(java.lang.String)
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
                } catch (MetaMatrixProcessingException e) {
                    exceptionWrapper = accumulateProcessingException(exceptionWrapper, e);
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
     * @see com.metamatrix.admin.api.core.CoreRuntimeStateAdmin#stopConnectorBinding(java.lang.String, boolean)
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
                } catch (MetaMatrixProcessingException e) {
                    exceptionWrapper = accumulateProcessingException(exceptionWrapper, e);
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
     * @see com.metamatrix.admin.api.core.CoreRuntimeStateAdmin#clearCache(java.lang.String)
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
                    	throw createSystemException(e);
                    } catch (MetaMatrixProcessingException e) {
                    	throw createProcessingException(e);
                    }                                        
                } 
            }
        }
        
        if (!processed) {
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.invalid_request", new Object[] {identifier})); //$NON-NLS-1$
        }        
    }
    
    /** 
     * @see com.metamatrix.admin.api.core.CoreRuntimeStateAdmin#terminateSession(java.lang.String)
     * @since 4.3
     */
    public void terminateSession(String identifier) 
        throws AdminException {
        
        if (identifier == null || (!identifier.equals(AdminObject.WILDCARD) && !identifier.matches("\\d+"))) { //$NON-NLS-1$
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        Set<ServerConnection> connections = getClientConnections();
        ArrayList matchedConnections = new ArrayList();
        for (Iterator i = connections.iterator(); i.hasNext();) {
        	ServerConnection clientConnection = (ServerConnection)i.next();
            String id = clientConnection.getLogonResult().getSessionID().toString();
            if (matches(identifier, id)) {
                matchedConnections.add(clientConnection);
            }
        }

        // Double iteration because to avoid concurrent modification of underlaying map.
        for (Iterator i = matchedConnections.iterator(); i.hasNext();) {
        	ServerConnection clientConnection = (ServerConnection)i.next();
        
        	try {
				this.manager.getDQP().terminateConnection(clientConnection.getLogonResult().getSessionID().toString());
			} catch (MetaMatrixComponentException e) {
				throw createSystemException(e);
			}
			
            // Shutdown the connection
            clientConnection.shutdown();
        }
    }
    
    /** 
     * @see com.metamatrix.admin.api.core.CoreRuntimeStateAdmin#cancelRequest(java.lang.String)
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
		} catch (MetaMatrixProcessingException e) {
			throw createProcessingException(e);
		} catch (MetaMatrixComponentException e) {
			throw createSystemException(e);
		}
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreRuntimeStateAdmin#cancelSourceRequest(java.lang.String)
     * @since 4.3
     */
    public void cancelSourceRequest(String identifier) 
        throws AdminException {
        
        if (identifier == null || !identifier.matches("\\d+\\" + Request.DELIMITER + "\\d+\\" + Request.DELIMITER + "\\d+")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            throw new AdminProcessingException(DQPEmbeddedPlugin.Util.getString("Admin.Invalid_identifier")); //$NON-NLS-1$                
        }
        
        String[] identifierParts = MMRequest.buildIdentifierArray(identifier);

        String connId = identifierParts[0];
        long requestId = Long.parseLong(identifierParts[1]);
        int nodeId = Integer.parseInt(identifierParts[2]);
        RequestID id = new RequestID(connId, requestId);

        try {
            this.manager.getDQP().cancelAtomicRequest(id, nodeId);
		} catch (MetaMatrixComponentException e) {
			throw createSystemException(e);
		}
    }

    /** 
     * @see com.metamatrix.admin.api.core.CoreRuntimeStateAdmin#changeVDBStatus(java.lang.String, java.lang.String, int)
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
        	throw createSystemException(e);
        } catch (MetaMatrixComponentException e) {
        	throw createSystemException(e);
        } catch (MetaMatrixProcessingException err) {
        	throw createProcessingException(err);
        }
    }
    
    
    /** 
     * @see com.metamatrix.admin.api.embedded.EmbeddedRuntimeStateAdmin#setLogListener(java.lang.Object)
     * @since 4.3
     */
    public void setLogListener(EmbeddedLogger listener) 
        throws AdminException {
        if(listener != null) {
            try{
                EmbeddedConfigUtil.installLogListener(new DQPLogListener(listener));
            }catch(MetaMatrixComponentException e) {
            	throw createProcessingException(e);
            }
        }
        else {
            throw new AdminProcessingException("Admin_invalid_log_listener"); //$NON-NLS-1$
        }
    }
}
