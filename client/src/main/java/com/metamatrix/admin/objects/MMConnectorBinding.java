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

import org.teiid.adminapi.ConnectorBinding;

import com.metamatrix.admin.AdminPlugin;


/**
 * A Connector Binding is a Connector Type with properties that have been 
 * bond to a Connector.
 * 
 * May are may not be assigned to a VDB
 */
public class MMConnectorBinding extends MMAdminObject implements ConnectorBinding {
	
    private String routingUUID = "";  //$NON-NLS-1$
    private String description = ""; //$NON-NLS-1$
    private String connectorTypeName = ""; //$NON-NLS-1$
    private int currentState;
    private Date stateChangedTime;
    private long serviceID = -1;
    
	
    /**
     * Constructor.
     * @param identifierParts
     * @since 4.3
     */
    public MMConnectorBinding(String[] identifierParts) {
        super(identifierParts);
    }

    

	

	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMConnectorBinding.MMConnectorBinding")).append(getIdentifier()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.Description")).append(description); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.Created")).append(getCreatedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.Created_By")).append(getCreatedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.Updated")).append(getLastChangedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.Updated_By")).append(getLastChangedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.State")).append(getState()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.State_Changed")).append(getStateChangedTime()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.IsRegistered")).append(isRegistered()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMConnectorBinding.Properties")).append(getPropertiesAsString()); //$NON-NLS-1$
		return result.toString();
	}

	
   
    /**
     * Returns the description 
     * @return description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Set the description
     * @param description
     */
    public void setDescription(String description) {
        this.description = description;
    }
       
    
    
    /** 
     * Returns the String globally unique routing UUID for this
     * Connector Binding 
     * @return Returns the routingUUID.
     * @since 4.3
     */
    public String getRoutingUUID() {
        return this.routingUUID;
    }

    /** 
     * @param routingUUID The routingUUID to set.
     * @since 4.3
     */
    public void setRoutingUUID(String routingUUID) {
        this.routingUUID = routingUUID;
    }
    
    /**
     * @return the current state of this connector binding.
     */
    public int getState() {
        return this.currentState;
    }
    
    /** 
     * @return Returns the currentState as String.
     * @since 4.3
     */
    public String getStateAsString() {
        switch (currentState) {
            case STATE_OPEN:
                return AdminPlugin.Util.getString("MMConnectorBinding.open"); //$NON-NLS-1$
            case STATE_CLOSED:
                return AdminPlugin.Util.getString("MMConnectorBinding.closed"); //$NON-NLS-1$
            case STATE_FAILED:
                return AdminPlugin.Util.getString("MMConnectorBinding.failed"); //$NON-NLS-1$
            case STATE_INIT_FAILED:
                return AdminPlugin.Util.getString("MMConnectorBinding.initializationFailed"); //$NON-NLS-1$
            case STATE_NOT_INITIALIZED:
                return AdminPlugin.Util.getString("MMConnectorBinding.notInitialized"); //$NON-NLS-1$
            case STATE_NOT_REGISTERED:
                return AdminPlugin.Util.getString("MMConnectorBinding.notRegistered"); //$NON-NLS-1$
            case STATE_DATA_SOURCE_UNAVAILABLE:
                return AdminPlugin.Util.getString("MMConnectorBinding.dataSourceUnavailable"); //$NON-NLS-1$
            default:
                return AdminPlugin.Util.getString("MMConnectorBinding.unknown"); //$NON-NLS-1$            
        }
    }
    
    /**
     * Set the state 
     * @param state
     * @since 4.3
     */
    public void setState(int state) {
        this.currentState = state;

        //check on what states mean "registered"
        setRegistered(currentState==STATE_OPEN || currentState==STATE_FAILED || currentState==STATE_DATA_SOURCE_UNAVAILABLE);
    }
    
    
    /** 
     * @return Returns time of last state change.
     * @since 4.3
     */
    public Date getStateChangedTime() {
        return stateChangedTime;        
    }
    
    /**
     * Set the state changed time 
     * @param stateChangedTime
     * @since 4.3
     */
    public void setStateChangedTime(Date stateChangedTime) {
        this.stateChangedTime = stateChangedTime;
    }
    
    
    /** 
     * @return Returns the serviceID.
     * @since 4.3
     */
    public long getServiceID() {
        return this.serviceID;
    }
    
    /** 
     * @param serviceID The serviceID to set.
     * @since 4.3
     */
    public void setServiceID(long serviceID) {
        this.serviceID = serviceID;
    }
    

    /** 
     * @return Returns the processID.
     * @since 4.3
     */
    public String getProcessName() {
        return identifierParts[1];
    }

    
    /** 
     * @return Returns the hostName.
     * @since 4.3
     */
    public String getHostName() {
        return identifierParts[0];
    }


    /** 
     * @param connectorTypeName the identifier for a connector type
     * @since 4.3
     */
    public void setConnectorTypeName(String connectorTypeName) {
        this.connectorTypeName = connectorTypeName;
    }

    /** 
     * @see org.teiid.adminapi.ConnectorBinding#getConnectorTypeName()
     * @since 4.3
     */
    public String getConnectorTypeName() {
        return this.connectorTypeName;
    }
}