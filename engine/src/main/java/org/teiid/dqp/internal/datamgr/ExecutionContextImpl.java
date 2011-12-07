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
import java.lang.ref.WeakReference;
import java.security.Principal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.Subject;

import org.teiid.adminapi.Session;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.message.RequestID;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionContext;


/**
 */
public class ExecutionContextImpl implements ExecutionContext {
    //  Access Node ID
    private String partID;
    // currentConnector ID
    private String connectorName;    
    // Execute count of the query
    private String executeCount;
    // keep the execution object alive during the processing. default:false 
    private boolean keepAlive = false;
    
    private boolean isTransactional;
    
    private int batchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
	private List<Exception> warnings = new LinkedList<Exception>();
	private Session session;
	private WeakReference<RequestWorkItem> worktItem;
	private boolean dataAvailable;
	private String generalHint;
	private String hint;
	private CommandContext commandContext;
	
	public ExecutionContextImpl(String vdbName, int vdbVersion,  Serializable executionPayload, 
            String originalConnectionID, String connectorName, long requestId, String partId, String execCount) {
		commandContext = new CommandContext();
		commandContext.setVdbName(vdbName);
		commandContext.setVdbVersion(vdbVersion);
		commandContext.setCommandPayload(executionPayload);
		commandContext.setConnectionID(originalConnectionID);
		commandContext.setRequestId(new RequestID(originalConnectionID, requestId));
		this.connectorName = connectorName;
        this.partID = partId;        
        this.executeCount = execCount;
	}
    
    public ExecutionContextImpl(CommandContext commandContext, String connectorName, String partId, String execCount) {
        this.connectorName = connectorName;
        this.partID = partId;        
        this.executeCount = execCount;
        this.commandContext = commandContext;
    }
    
    @Override
    public org.teiid.CommandContext getCommandContext() {
    	return this.commandContext;
    }
    
    public String getConnectorIdentifier() {
        return this.connectorName;
    }
    
    @Override
    public String getRequestIdentifier() {
        return this.commandContext.getRequestId();
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
        return this.commandContext.getVdbName();
    }
    @Override
    public int getVirtualDatabaseVersion() {
        return this.commandContext.getVdbVersion();
    }
    @Override
    public Subject getSubject() {
        return this.commandContext.getSubject();
    }
    
    @Override
    public Serializable getExecutionPayload() {
        return this.commandContext.getCommandPayload();
    }
    
    @Override
	public String getConnectionIdentifier() {
		return this.commandContext.getConnectionID();
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
        return HashCodeUtil.hashCode(HashCodeUtil.hashCode(0, getRequestIdentifier()), partID);
    }

    public String toString() {
    	String userName = null;
    	if (this.getSubject() != null) {
	    	for(Principal p:this.getSubject().getPrincipals()) {
	    		userName = p.getName();
	    	}
    	}
        return "ExecutionContext<vdb=" + this.getVirtualDatabaseName() + ", version=" + this.getVirtualDatabaseVersion() + ", user=" + userName + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
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
		this.worktItem = new WeakReference<RequestWorkItem>(item);
	}
	
	@Override
	public synchronized void dataAvailable() {
		RequestWorkItem requestWorkItem = this.worktItem.get();
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
