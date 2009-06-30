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

package com.metamatrix.admin.objects;

import java.util.Date;

import org.teiid.adminapi.Request;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.core.util.DateUtil;

/**
 * Dataholder for information about Requests.
 */
public class MMRequest extends MMAdminObject implements Request{
	
	String userName = ""; //$NON-NLS-1$;
	String sessionID;
    String requestID;
	Date created;
	Date processingDate;
	String transactionID = ""; //$NON-NLS-1$;
	String sqlCommand = ""; //$NON-NLS-1$; 
    String connectorBindingName= ""; //$NON-NLS-1$; 
    String nodeID=""; //$NON-NLS-1$
    boolean sourceRequest = false;
    
    /**
     * Construct a new MMRequest 
     * @param identifierParts
     * @since 4.3
     */
    public MMRequest(String[] identifierParts) {
        super(identifierParts);
        
        this.sessionID = identifierParts[0];
        this.requestID = identifierParts[1];
        if (identifierParts.length >= 3) {
            this.nodeID = identifierParts[2];
            this.sourceRequest = true;
        }
        this.name = requestID;
    }
    
    
	
    
    /**
     * @return Date the request was created
     * @since 4.3
     */
	public Date getCreated() {
        return created;
    }
    
    
    /**
     * @return Date the request was created, as a String
     * @since 4.3
     */
	public String getCreatedString() {
        String result = ""; //$NON-NLS-1$;
        if( created != null ) {
            result = DateUtil.getDateAsString(created);
        } 
        return result;
	}
	
    
    
    /**
     * Get the RequestID for a Request
     * @return RequestID
     */
    public String getRequestID() {
        return requestID;
    } 
    
	/**
	 * Get the SessionID for a Request
	 * 
	 * @return long SessionID
	 */
	public String getSessionID() {
		return sessionID;
	}

	/**
	 * Get the SQL Command sent to the Server for a Request
	 * 
	 * @return MetaMatrix SQL Command
	 */
	public String getSqlCommand() {
		return sqlCommand;
	}

	/**
	 * Get the Date processing began for the Request
	 * @return Date processing began for the request
	 */
	public Date getProcessingDate() {
		return processingDate;
	}
    
    /**
     * Get the Date processing began for the Request, as a String
     * @return Date processing began for the request
     */
    public String getProcessingDateString() {
        String result = ""; //$NON-NLS-1$;
        if (processingDate != null) {
            result = DateUtil.getDateAsString(processingDate);
        }
        return result;
    }
    

	/**
	 * Get the TransactionID of the Request
	 * 
	 * @return String of TransactionID if in a transaction
	 */
	public String getTransactionID() {
		return transactionID;
	}

	/**
	 * Get the UserName of the Request
	 * 
	 * @return String username for the Request
	 */
	public String getUserName() {
		return userName;
	}
    
    
    
    
    
    /** 
     * @param created The date created.
     * @since 4.3
     */
    public void setCreated(Date created) {
        this.created = created;
    }
    /** 
     * @param sessionID The sessionID to set.
     * @since 4.3
     */
    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }
    /** 
     * @param sqlCommand The sqlCommand to set.
     * @since 4.3
     */
    public void setSqlCommand(String sqlCommand) {
        this.sqlCommand = sqlCommand;
    }
    /** 
     * @param processingDate The date processing began.
     * @since 4.3
     */
    public void setProcessingDate(Date processingDate) {
        this.processingDate = processingDate;
    }
    /** 
     * @param transactionID The transactionID to set.
     * @since 4.3
     */
    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }
    /** 
     * @param userName The userName to set.
     * @since 4.3
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }
    
    /** 
     * @return Returns the connectorBindingName.
     * @since 4.3
     */
    public String getConnectorBindingName() {
        return this.connectorBindingName;
    }
    /** 
     * @param connectorBindingName The connectorBindingName to set.
     * @since 4.3
     */
    public void setConnectorBindingName(String connectorBindingName) {
        this.connectorBindingName = connectorBindingName;
    }
    
    /** 
     * @return Returns whether this is a Source Request.
     * @since 4.3
     */
    public boolean isSource() {
        return sourceRequest;
    }
    
    /**
     * Set if the request is source request 
     * @param value
     * @since 4.3
     */
    public void setSource(boolean value) {
        sourceRequest = value;
    }
    
    /**
     * If  this is a source request then this represents the node id 
     * @return Returns the nodeID.
     * @since 4.3
     */
    public String getNodeID() {
        return this.nodeID;
    }
    
    /** 
     * If  this is a source request then this represents the node id
     * @param nodeID The nodeID to set.
     * @since 4.3
     */
    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }    
    
    
	/**
	 * @see java.lang.Object#toString()
     * @return String for display purposes
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMRequest.MMRequest")).append(getIdentifier());  //$NON-NLS-1$
        if (isSource()) {
            result.append(AdminPlugin.Util.getString("MMRequest.nodeID")).append(getNodeID());  //$NON-NLS-1$    
        }
		result.append(AdminPlugin.Util.getString("MMRequest.requestID")).append(getRequestID()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.userName")).append(userName); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.sessionID")).append(sessionID); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.created")).append(getCreatedString()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.processing")).append(getProcessingDateString()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.transactionID")).append(transactionID); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMRequest.command")).append(sqlCommand);  //$NON-NLS-1$
		return result.toString();
	}   
}
