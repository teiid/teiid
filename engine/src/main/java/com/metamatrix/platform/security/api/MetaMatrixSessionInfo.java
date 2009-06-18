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

package com.metamatrix.platform.security.api;

import java.io.Serializable;
import java.util.Properties;

/**
 * This class represents an immutable informational object describing
 * the attributes of a unique MetaMatrix session within a given MetaMatrix System.
 */
public class MetaMatrixSessionInfo implements Serializable, Cloneable {
    
    public final static long serialVersionUID = -9120197553960136239L;
    
    private SessionToken sessionToken;  // immutable
    private long lastPingTime;
    private long timeCreated;
    private String applicationName;
    private Properties productInfo;
    private String clientIp;
    private String clientHostname;
    private Serializable trustedToken;

    /**
     * Master constructor, allows a MetaMatrixSessionInfo to be created with
     * any state and any timestamps.
     */
    public MetaMatrixSessionInfo(MetaMatrixSessionID sessionID, String userName, long timeCreated, String applicationName, Properties productInfo, String clientIp, String clientHostname){
        this.timeCreated = timeCreated;
        this.lastPingTime = timeCreated;
        this.applicationName = applicationName;
        this.sessionToken = new SessionToken(sessionID, userName);
        this.productInfo = productInfo;
        this.clientIp = clientIp;
        this.clientHostname = clientHostname;
    }

    public MetaMatrixSessionID getSessionID() {
        return this.sessionToken.getSessionID();
    }

    public String getUserName() {
        return this.sessionToken.getUsername();
    }

    public String getApplicationName() {
        return this.applicationName;
    }

    public long getTimeCreated() {
        return this.timeCreated;
    }

    /**
     * Get the time the server was last pinged by this session.
     * Note that the session's "last ping time" will only be acurate
     * if the session is in the ACTIVE state.
     * @return The time the server was last pinged by this session.
     */
    public long getLastPingTime() {
        return lastPingTime;
    }

    /**
     * Used <i><b>ONLY</b></i> by the session service to set the
     * time this session last initiated a server ping.
     * @param lastPingTime The last time this session pinged the server.
     */
    public void setLastPingTime(long lastPingTime) {
        this.lastPingTime = lastPingTime;
    }

    public SessionToken getSessionToken(){
        return this.sessionToken;
    }

    /**
     * Return a cloned instance of this object.
     * @return the object that is the clone of this instance.
     */
    public Object clone() {
        try {
            // Everything is immutable, so bit-wise copy (of references) is okay!
            return super.clone();
        } catch ( CloneNotSupportedException e ) {
        }
        return null;
    }

    /**
     * Returns a string representing the current state of the object.
     */
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("MetaMatrixSessionInfo[ "); //$NON-NLS-1$
        s.append(this.sessionToken.toString());
        s.append(", "); //$NON-NLS-1$
        s.append("application:"); //$NON-NLS-1$
        s.append(this.applicationName);
        s.append(", created:"); //$NON-NLS-1$
        s.append(this.timeCreated);
        s.append(", last pinged server:"); //$NON-NLS-1$
        s.append(this.lastPingTime);
        s.append("]"); //$NON-NLS-1$
        return s.toString();
    }

    /** 
     * @return Returns the productInfo.
     * @since 4.3
     */
    public String getProductInfo(String key) {
        return this.productInfo.getProperty(key);
    }
    
    public Properties getProductInfo() {
    	return this.productInfo;
    }

	public String getClientIp() {
		return clientIp;
	}

	public String getClientHostname() {
		return clientHostname;
	}

	public void setTrustedToken(Serializable trustedToken) {
		this.trustedToken = trustedToken;
	}

	public Serializable getTrustedToken() {
		return trustedToken;
	}
}
