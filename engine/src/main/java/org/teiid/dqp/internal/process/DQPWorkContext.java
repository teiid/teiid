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

import javax.security.auth.Subject;

import org.teiid.adminapi.impl.VDBMetaData;

import com.metamatrix.dqp.message.RequestID;
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

	public static void releaseWorkContext() {
		CONTEXTS.set(null);
	}	
	
    private String vdbName;
    private int vdbVersion;
    private String appName;
    private SessionToken sessionToken;
    private String clientAddress;
    private String clientHostname;
    private Subject subject;
	private String securityDomain;
	private Object securityContext;
	private VDBMetaData vdb;
	private boolean admin;
    
    public DQPWorkContext() {
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
    
    public Subject getSubject() {
        return this.subject;
    }
    
    public void setSubject(Subject subject) {
        this.subject = subject;
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
    public int getVdbVersion() {
        return vdbVersion;
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
    public void setVdbVersion(int vdbVersion) {
        this.vdbVersion = vdbVersion;
    }

	public String getConnectionID() {
		return String.valueOf(getSessionId());
	}
	
	public long getSessionId() {
		if (this.sessionToken == null) {
			return -1;
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
	
	public void reset() {
		setSessionToken(null);
		setAppName(null);
		setVdbName(null);
		setVdbVersion(0);
		setSecurityContext(null);
		setSecurityDomain(null);
		setVdb(null);
		setSubject(null);
		setSessionToken(null);
	}

	public void setSecurityDomain(String securityDomain) {
		this.securityDomain = securityDomain;
	}
	
	public String getSecurityDomain() {
		return this.securityDomain;
	}

	public Object getSecurityContext() {
		return this.securityContext;
	}
	
	public void setSecurityContext(Object securityContext) {
		this.securityContext = securityContext;
	}

	public void setVdb(VDBMetaData vdb) {
		this.vdb = vdb;
	}
	
	public VDBMetaData getVDB() {
		return vdb;
	}

	public void markAsAdmin() {
		this.admin = true;
	}
	
	public boolean isAdmin() {
		return this.admin;
	}
}
