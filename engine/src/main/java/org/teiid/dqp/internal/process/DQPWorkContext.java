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

package org.teiid.dqp.internal.process;

import java.io.Serializable;

import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;

public class DQPWorkContext implements Serializable {
	
	private static final long serialVersionUID = -6389893410233192977L;
	
	private static ThreadLocal<DQPWorkContext> CONTEXTS = new ThreadLocal<DQPWorkContext>() {
		protected DQPWorkContext initialValue() {
			return new DQPWorkContext();
		}
	};

	public static DQPWorkContext getWorkContext() {
		return CONTEXTS.get();
	}
	
	public static void setWorkContext(DQPWorkContext context) {
		CONTEXTS.set(context);
	}
	
    private Serializable trustedPayload;
    private String vdbName;
    private String vdbVersion;
    private String appName;
    private SessionToken sessionToken;
    private String clientAddress;
    private String clientHostname;
    
    public DQPWorkContext() {
	}

    /**
     * @return
     */
    public Serializable getTrustedPayload() {
        return trustedPayload;
    }

    /**
     * @return
     */
    public String getUserName() {
		if (this.sessionToken == null) {
			return null;
		}
        return this.sessionToken.getUsername();
    }

    /**
     * @return
     */
    public String getVdbName() {
        return vdbName;
    }

    /**
     * @return
     */
    public String getVdbVersion() {
        return vdbVersion;
    }

    /**
     * @param serializable
     */
    public void setTrustedPayload(Serializable trustedPayload) {
        this.trustedPayload = trustedPayload;
    }

    /**
     * @param string
     */
    public void setVdbName(String vdbName) {
        this.vdbName = vdbName;
    }

    /**
     * @param string
     */
    public void setVdbVersion(String vdbVersion) {
        this.vdbVersion = vdbVersion;
    }

	public String getConnectionID() {
		if (this.sessionToken == null) {
			return null;
		}
		return this.sessionToken.getSessionIDValue();
	}
	
	public MetaMatrixSessionID getSessionId() {
		if (this.sessionToken == null) {
			return null;
		}
		return this.sessionToken.getSessionID();
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppName() {
		return appName;
	}
	
	public RequestID getRequestID(long exeuctionId) {
		return new RequestID(this.getConnectionID(), exeuctionId);
	}
	
	public void setSessionToken(SessionToken sessionToken) {
		this.sessionToken = sessionToken;
	}

	public SessionToken getSessionToken() {
		return sessionToken;
	}

	public void setClientAddress(String clientAddress) {
		this.clientAddress = clientAddress;
	}

	public String getClientAddress() {
		return clientAddress;
	}

	public void setClientHostname(String clientHostname) {
		this.clientHostname = clientHostname;
	}

	public String getClientHostname() {
		return clientHostname;
	}

}
