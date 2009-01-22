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

package com.metamatrix.dqp.internal.datamgr.impl;

import java.io.Serializable;

import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;

/**
 */
public class ExecutionContextImpl implements SecurityContext {

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
    // uses the result set chache or not
	private boolean useResultSetCache;
    // Execute count of the query
    private String executeCount;
    // keep the execution object alive during the processing. default:false 
    private boolean keepAlive = false;
    
    private boolean isTransactional;
    
    public ExecutionContextImpl(String vdbName, String vdbVersion, String userName,
                                Serializable trustedPayload, Serializable executionPayload, 
                                String originalConnectionID, String connectorId, String requestId, String partId, String execCount, boolean useResultSetCache) {
        
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
        this.useResultSetCache = useResultSetCache;        
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

	public boolean useResultSetCache() {
		return useResultSetCache;
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
        } else if(obj == null || ! (obj instanceof ExecutionContext)) {
            return false;
        } else {
            ExecutionContext other = (ExecutionContext) obj;
            return compareWithNull(this.getRequestIdentifier(), other.getRequestIdentifier()) && 
                    compareWithNull(this.getPartIdentifier(), other.getPartIdentifier());
        }
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

	boolean isTransactional() {
		return isTransactional;
	}

	void setTransactional(boolean isTransactional) {
		this.isTransactional = isTransactional;
	}
}
