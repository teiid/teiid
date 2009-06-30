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

import org.teiid.adminapi.Session;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.core.util.DateUtil;

/**
 * MetaMatrix Session 
 * 
 */
public class MMSession extends MMAdminObject implements Session {
	private String userName = ""; //$NON-NLS-1$;
	private String applicationName = ""; //$NON-NLS-1$
	private String sessionID;
	private String vdbName = ""; //$NON-NLS-1$
	private String vdbVersion = ""; //$NON-NLS-1$
    private String ipAddress = ""; //$NON-NLS-1$
    private String hostName = ""; //$NON-NLS-1$
	private long lastPingTime;
	private int sessionState;

  
    
    /**
     * constructor
     * @param identifierParts
     */
	public MMSession(String[] identifierParts) {
        super(identifierParts);
        this.sessionID = getIdentifier();
	}

   
	/**
     * Convert a Session to a String 
     *  
	 * @see java.lang.Object#toString()
	 * @since 4.3
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMSession.MMSession")).append(getIdentifier()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMSession.User_Name")).append(userName); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMSession.Application")).append(applicationName); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMSession.ID")).append(sessionID); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMSession.VDB_Name")).append(vdbName); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMSession.VDB_Version")).append(vdbVersion); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMSession.Last_Ping_Time")).append(getLastPingTimeString()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMSession.State")).append(getStateAsString()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMSession.IPAddress")).append(ipAddress); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMSession.HostName")).append(hostName); //$NON-NLS-1$      
        
		return result.toString();
	}

	/**
	 * Get the Last time Client has checked to see if the server is still available
	 * 
	 * @return Date of the last ping to the server.
	 */
	public Date getLastPingTime() {
		return new Date(lastPingTime);
	}
    
    
    /**
     * Get the Time the User logged into the System as a String
     * 
     * @return String 
     */
    public String getLastPingTimeString() {
        return DateUtil.getDateAsString(getLastPingTime());
    }
    
    /**
     * Set the Last time Client has checked to see if the server is still available  
     * @param lastPingTime
     */
    public void setLastPingTime(long lastPingTime) {
        this.lastPingTime = lastPingTime;
    }
	
    
	
	/**
	 * Get the SessionState
	 * 
	 * @return String with the SessionState
	 */
	public String getStateAsString() {
		String result = ""; //$NON-NLS-1$
		switch (sessionState) {
			case STATE_EXPIRED :
				result = EXPIRED_STATE_DESC; 
				break;
			case STATE_ACTIVE :
				result = ACTIVE_STATE_DESC; 
				break;
			case STATE_CLOSED :
				result = CLOSED_STATE_DESC; 
				break;
			case STATE_TERMINATED :
				result = TERMINATED_STATE_DESC; 
				break;
            case STATE_PASSIVATED :
                result = PASSIVATED_STATE_DESC; 
                break;                
			default :
				result = UNKNOWN_STATE_DESC; 
		}
		return result;
	}
    
    
    /**
     * Set the SessionState
     * @param state
     */
    public void setSessionState(int state) {
        this.sessionState = state;
    }

	/**
	 * Get the Application Name
	 * 
	 * @return String of the Application Name
	 */
	public String getApplicationName() {
		return applicationName;
	}

    /**
     * Set the ApplicationName
     * @param name
     */
    public void setApplicationName(String name) {
        this.applicationName = name;
    }
    
	/**
	 * Get the unique MetaMatrix session
     * within a given MetaMatrix System
	 * 
	 * @return Strings of the Session ID
	 */
	public String getSessionID() {
		return sessionID;
	}

	/**
	 * Get the State of the Session 
	 * 
	 * @return int of the Session's state
	 */
	public int getState() {
		return sessionState;
	}

	/**
	 * Get User Name for this Session
	 * 
	 * @return String of UserName
	 */
	public String getUserName() {
		return userName;
	}
    
    /**
     * Set User Name for this Session
     * @param userName
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

	/**
	 * Get the VDB Name for this Session
	 * 
	 * @return String name of the VDB
	 */
	public String getVDBName() {
		return vdbName;
	}

    /**
     * Set VDBName for this Session
     * @param vdbName
     */
    public void setVDBName(String vdbName) {
        this.vdbName = vdbName;
    }
    
	/**
	 * Get the VDB Version for this Session
	 * 
	 * @return String name/number of the VDB Version
	 */
	public String getVDBVersion() {
		return vdbVersion;
	}

     /**
     * Set VDBVersion for this Session
     * @param vdbVersion
     */
    public void setVDBVersion(String vdbVersion) {
        this.vdbVersion = vdbVersion;
    }
    
    /**
     * Set IPAddress for this Session
     * @param ipAddress
     */
    public void setIPAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    } 
    
 
    /**
     * Get the IPAddress for this Session
     * @return IPAddress
     */
    public String getIPAddress() {
        return this.ipAddress;
    }
    
    /**
     * Set information defined by the client at runtime
     * @param clientMachineName
     */
    public void setHostName(String clientMachineName) {
        this.hostName = clientMachineName;
    } 
    
 
    /**
     * Get the host name of the machine the client is 
     * accessing from
     * @return IPAddress
     */
    public String getHostName() {
        return this.hostName;
    }    
    

}
