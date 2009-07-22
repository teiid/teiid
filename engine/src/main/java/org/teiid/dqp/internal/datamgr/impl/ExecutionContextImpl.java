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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.teiid.connector.api.ConnectorIdentity;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.dqp.internal.cache.DQPContextCache;

import com.metamatrix.cache.Cache;
import com.metamatrix.common.buffer.impl.BufferConfig;
import com.metamatrix.core.util.HashCodeUtil;

/**
 */
public class ExecutionContextImpl implements ExecutionContext {

    // Orginal request non-atomic request id
    private String requestID;   
    //  Access Node ID
    private String partID;
    // currentConnector ID
    private String connectorID;    
    // current VDB 
    private String vdbName;
    // Current VDB's version
    private String vdbVersion;
    // User Name
    private String userName;
    // Payload setup on the Connection Object
    private Serializable trustedPayload;
    // Payload setup on the Statement object
    private Serializable executionPayload;
    // ID of the parent JDBC Connection which is executing the statement
    private String requestConnectionID;
    // Execute count of the query
    private String executeCount;
    // keep the execution object alive during the processing. default:false 
    private boolean keepAlive = false;
    
    private boolean isTransactional;
    
    private ConnectorIdentity connectorIdentity;
    
    private DQPContextCache contextCache;
    
    private int batchSize = BufferConfig.DEFAULT_CONNECTOR_BATCH_SIZE;
	private List<Exception> warnings = new LinkedList<Exception>();
    
    public ExecutionContextImpl(String vdbName, String vdbVersion, String userName,
                                Serializable trustedPayload, Serializable executionPayload, 
                                String originalConnectionID, String connectorId, String requestId, String partId, String execCount) {
        
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.userName = userName;
        this.trustedPayload = trustedPayload;
        this.executionPayload = executionPayload;
        this.connectorID = connectorId;
        this.requestID = requestId;
        this.partID = partId;        
        this.requestConnectionID = originalConnectionID;
        this.executeCount = execCount;
    }
    
    public String getConnectorIdentifier() {
        return this.connectorID;
    }
    
    public String getRequestIdentifier() {
        return this.requestID;
    }

    public String getPartIdentifier() {
        return this.partID;
    }
    public String getExecutionCountIdentifier() {
        return this.executeCount;
    }
    public String getVirtualDatabaseName() {
        return this.vdbName;
    }

    public String getVirtualDatabaseVersion() {
        return this.vdbVersion;
    }

    public String getUser() {
        return this.userName;
    }

    public Serializable getTrustedPayload() {
        return this.trustedPayload;
    }

    public Serializable getExecutionPayload() {
        return executionPayload;
    }
    
	public String getConnectionIdentifier() {
		return requestConnectionID;
	}

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
        return "ExecutionContext<vdb=" + this.vdbName + ", version=" + this.vdbVersion + ", user=" + this.userName + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

	public boolean isTransactional() {
		return isTransactional;
	}

	void setTransactional(boolean isTransactional) {
		this.isTransactional = isTransactional;
	}

	@Override
	public ConnectorIdentity getConnectorIdentity() {
		return this.connectorIdentity;
	}
	
	public void setConnectorIdentity(ConnectorIdentity connectorIdentity) {
		this.connectorIdentity = connectorIdentity;
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
}
