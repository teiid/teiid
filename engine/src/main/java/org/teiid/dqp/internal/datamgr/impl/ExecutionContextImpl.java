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

package org.teiid.dqp.internal.datamgr.impl;

import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.Subject;

import org.teiid.cache.Cache;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.dqp.DQPPlugin;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.resource.cci.CacheScope;
import org.teiid.resource.cci.ExecutionContext;


/**
 */
public class ExecutionContextImpl implements ExecutionContext {

    // Orginal request non-atomic request id
    private String requestID;   
    //  Access Node ID
    private String partID;
    // currentConnector ID
    private String connectorName;    
    // current VDB 
    private String vdbName;
    // Current VDB's version
    private int vdbVersion;
    // User Name
    private Subject user;
    // Payload setup on the Statement object
    private Serializable executionPayload;
    // ID of the parent JDBC Connection which is executing the statement
    private String requestConnectionID;
    // Execute count of the query
    private String executeCount;
    // keep the execution object alive during the processing. default:false 
    private boolean keepAlive = false;
    
    private boolean isTransactional;
    private DQPContextCache contextCache;
    
    private int batchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
	private List<Exception> warnings = new LinkedList<Exception>();
    
    public ExecutionContextImpl(String vdbName, int vdbVersion,  Serializable executionPayload, 
                                String originalConnectionID, String connectorName, String requestId, String partId, String execCount) {
        
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.executionPayload = executionPayload;
        this.connectorName = connectorName;
        this.requestID = requestId;
        this.partID = partId;        
        this.requestConnectionID = originalConnectionID;
        this.executeCount = execCount;
    }
    
    public String getConnectorIdentifier() {
        return this.connectorName;
    }
    
    @Override
    public String getRequestIdentifier() {
        return this.requestID;
    }

    @Override
    public String getPartIdentifier() {
        return this.partID;
    }
    
    @Override
    public String getExecutionCountIdentifier() {
        return this.executeCount;
    }
    @Override
    public String getVirtualDatabaseName() {
        return this.vdbName;
    }
    @Override
    public int getVirtualDatabaseVersion() {
        return this.vdbVersion;
    }
    @Override
    public Subject getSubject() {
        return this.user;
    }
    
    public void setUser(Subject user) {
        this.user = user;
    }

    @Override
    public Serializable getExecutionPayload() {
        return executionPayload;
    }
    
    @Override
	public String getConnectionIdentifier() {
		return requestConnectionID;
	}
    @Override
    public void keepExecutionAlive(boolean alive) {
        this.keepAlive = alive;
    }    
    
    boolean keepExecutionAlive() {
        return this.keepAlive;
    }
    	
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        } 
        if(! (obj instanceof ExecutionContext)) {
            return false;
        } 
        ExecutionContext other = (ExecutionContext) obj;
        return compareWithNull(this.getRequestIdentifier(), other.getRequestIdentifier()) && 
                compareWithNull(this.getPartIdentifier(), other.getPartIdentifier());
    }

    private boolean compareWithNull(Object obj1, Object obj2) {
        if(obj1 == null) { 
            if(obj2 == null) {
                return true;
            }
            return false;
        }
        if(obj2 == null) {
            return false;
        }
        return obj1.equals(obj2);
    }

    public int hashCode() {
        return HashCodeUtil.hashCode(HashCodeUtil.hashCode(0, requestID), partID);
    }

    public String toString() {
    	String userName = null;
    	if (this.user != null) {
	    	for(Principal p:this.user.getPrincipals()) {
	    		userName = p.getName();
	    	}
    	}
        return "ExecutionContext<vdb=" + this.vdbName + ", version=" + this.vdbVersion + ", user=" + userName + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
	
    @Override
	public boolean isTransactional() {
		return isTransactional;
	}

	void setTransactional(boolean isTransactional) {
		this.isTransactional = isTransactional;
	}
		
	@Override
	public int getBatchSize() {
		return batchSize;
	}
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
		
	/**
	 * Add an exception as a warning to this Execution.
	 */
	@Override
	public void addWarning(Exception ex) {
		if (ex == null) {
			return;
		}
		this.warnings.add(ex);
	}
	
	public List<Exception> getWarnings() {
		List<Exception> result = new ArrayList<Exception>(warnings);
		warnings.clear();
		return result;
	}
	
	public void setContextCache(DQPContextCache cache) {
		this.contextCache = cache;
	}
	
	@Override
	public Object get(Object key) {
		if (this.contextCache != null) {
			Cache cache = contextCache.getRequestScopedCache(getRequestIdentifier());
			return cache.get(key);
		}
		return null;
	}
	
	@Override
	public void put(Object key, Object value) {
		if (this.contextCache != null) {
			Cache cache = contextCache.getRequestScopedCache(getRequestIdentifier());
			cache.put(key, value);
		}
	}	
	
	
	@Override
	public Object getFromCache(CacheScope scope, Object key) {
		DQPWorkContext context = DQPWorkContext.getWorkContext();
		checkScopeValidity(scope, context);

		Cache cache = getScopedCache(scope, context);
		if (cache != null) {
			return cache.get(key);
		}
		return null;
	}
	
	@Override
	public void storeInCache(CacheScope scope, Object key, Object value) {
		DQPWorkContext context = DQPWorkContext.getWorkContext();
		checkScopeValidity(scope, context);
		Cache cache = getScopedCache(scope, context);
		if (cache != null) {
			cache.put(key, value);
		}
	}
	
	private Cache getScopedCache(CacheScope scope, DQPWorkContext context) {
		switch (scope) {
			case SERVICE:
				return contextCache.getServiceScopedCache(getConnectorIdentifier());
			case SESSION:
				return contextCache.getSessionScopedCache(String.valueOf(context.getSessionToken().getSessionID()));
			case VDB:
				return contextCache.getVDBScopedCache(context.getVdbName(), context.getVdbVersion());
			case GLOBAL:
				return contextCache.getGlobalScopedCache();
		}
		return null;
	}
	
	private void checkScopeValidity(CacheScope scope, DQPWorkContext context) {
		if (scope == CacheScope.REQUEST) {
			throw new IllegalStateException(DQPPlugin.Util.getString("ConnectorEnvironmentImpl.request_scope_error")); //$NON-NLS-1$
		}
		
		if (scope == CacheScope.SESSION) {
			if (context == null || context.getSessionToken() == null) {
				throw new IllegalStateException(DQPPlugin.Util.getString("ConnectorEnvironmentImpl.session_scope_error")); //$NON-NLS-1$
			}
		}
		else if (scope == CacheScope.VDB) {
			if (context == null || context.getVdbName() == null || context.getVdbVersion() == 0) {
				throw new IllegalStateException(DQPPlugin.Util.getString("ConnectorEnvironmentImpl.vdb_scope_error")); //$NON-NLS-1$
			}
		}
	}	
}
