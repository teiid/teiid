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

package org.teiid.dqp.internal.datamgr;

import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.Subject;

import org.teiid.adminapi.Session;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.translator.ExecutionContext;


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
    
    private int batchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
	private List<Exception> warnings = new LinkedList<Exception>();
	private Session session;
	private RequestWorkItem worktItem;
	private boolean dataAvailable;
	private String generalHint;
	private String hint;
    
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
	
	@Override
	public Session getSession() {
		return this.session;
	}
	
	public void setSession(Session session) {
		this.session = session;
	}

	public void setRequestWorkItem(RequestWorkItem item) {
		this.worktItem = item;
	}
	
	@Override
	public synchronized void dataAvailable() {
		RequestWorkItem requestWorkItem = this.worktItem;
		dataAvailable = true;
		if (requestWorkItem != null) {
			requestWorkItem.moreWork();
		}
	}
	
	public synchronized boolean isDataAvailable() {
		boolean result = dataAvailable;
		dataAvailable = false;
		return result;
	}
	
	@Override
	public String getGeneralHint() {
		return generalHint;
	}
	
	@Override
	public String getSourceHint() {
		return hint;
	}
	
	public void setGeneralHint(String generalHint) {
		this.generalHint = generalHint;
	}
	
	public void setHint(String hint) {
		this.hint = hint;
	}
}
