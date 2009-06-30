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
import org.teiid.adminapi.Service;

import com.metamatrix.admin.AdminPlugin;


/**
 * A Service
 * 
 */
public class MMService extends MMAdminObject implements Service {
	
    private String description = ""; //$NON-NLS-1$
    private String componentTypeName = ""; //$NON-NLS-1$
    private int currentState;
    private Date stateChangedTime;
    private long serviceID = -1;
    
	
    /**
     * Constructor.
     * @param identifierParts
     * @since 6.1
     */
    public MMService(String[] identifierParts) {
        super(identifierParts);
    }

    

	

	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMService.MMService")).append(getIdentifier()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.Description")).append(description); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.Created")).append(getCreatedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.Created_By")).append(getCreatedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.Updated")).append(getLastChangedDate()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.Updated_By")).append(getLastChangedBy()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.State")).append(getState()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.State_Changed")).append(getStateChangedTime()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.IsRegistered")).append(isRegistered()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMService.Properties")).append(getPropertiesAsString()); //$NON-NLS-1$
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
     * @return the current state of this connector binding.
     */
    public int getState() {
        return this.currentState;
    }
    
    /** 
     * @return Returns the currentState as String.
     * @since 6.1
     */
    public String getStateAsString() {
        switch (currentState) {
            case STATE_OPEN:
                return AdminPlugin.Util.getString("MMService.open"); //$NON-NLS-1$
            case STATE_CLOSED:
                return AdminPlugin.Util.getString("MMService.closed"); //$NON-NLS-1$
            case STATE_FAILED:
                return AdminPlugin.Util.getString("MMService.failed"); //$NON-NLS-1$
            case STATE_INIT_FAILED:
                return AdminPlugin.Util.getString("MMService.initializationFailed"); //$NON-NLS-1$
            case STATE_NOT_INITIALIZED:
                return AdminPlugin.Util.getString("MMService.notInitialized"); //$NON-NLS-1$
            case STATE_NOT_REGISTERED:
                return AdminPlugin.Util.getString("MMService.notRegistered"); //$NON-NLS-1$
            case STATE_DATA_SOURCE_UNAVAILABLE:
                return AdminPlugin.Util.getString("MMService.dataSourceUnavailable"); //$NON-NLS-1$
            default:
                return AdminPlugin.Util.getString("MMService.unknown"); //$NON-NLS-1$            
        }
    }
    
    /**
     * Set the state 
     * @param state
     * @since 6.1
     */
    public void setState(int state) {
        this.currentState = state;

        //check on what states mean "registered"
        setRegistered(currentState==STATE_OPEN || currentState==STATE_FAILED || currentState==STATE_DATA_SOURCE_UNAVAILABLE);
    }
    
    
    /** 
     * @return Returns time of last state change.
     * @since 6.1
     */
    public Date getStateChangedTime() {
        return stateChangedTime;        
    }
    
    /**
     * Set the state changed time 
     * @param stateChangedTime
     * @since 6.1
     */
    public void setStateChangedTime(Date stateChangedTime) {
        this.stateChangedTime = stateChangedTime;
    }
    
    
    /** 
     * @return Returns the serviceID.
     * @since 6.1
     */
    public long getServiceID() {
        return this.serviceID;
    }
    
    /** 
     * @param serviceID The serviceID to set.
     * @since 6.1
     */
    public void setServiceID(long serviceID) {
        this.serviceID = serviceID;
    }
    

    /** 
     * @return Returns the processID.
     * @since 6.1
     */
    public String getProcessName() {
        return identifierParts[1];
    }

    
    /** 
     * @return Returns the hostName.
     * @since 6.1
     */
    public String getHostName() {
        return identifierParts[0];
    }


    /** 
     * @param connectorTypeName the identifier for a connector type
     * @since 6.1
     */
    public void setComponentTypeName(String componentTypeName) {
        this.componentTypeName = componentTypeName;
    }

    /** 
     * @see org.teiid.adminapi.Service#getComponentTypeName()
     * @since 6.1
     */
    public String getComponentTypeName() {
        return this.componentTypeName;
    }
}