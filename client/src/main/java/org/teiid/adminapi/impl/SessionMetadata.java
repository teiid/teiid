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
package org.teiid.adminapi.impl;

import java.util.Date;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.jboss.managed.api.annotation.ManagementProperty;
import org.jboss.metatype.api.annotations.MetaMapping;
import org.teiid.adminapi.Session;
import org.teiid.client.security.SessionToken;



/**
 * Add and delete properties also in the Mapper class for correct wrapping for profile service.
 * 
 */
/* TODO: it would probably be good to let ipAddress denote the connecting address 
 and add clientIpAdress as the client reported value */
@MetaMapping(SessionMetadataMapper.class)
public class SessionMetadata extends AdminObjectImpl implements Session {

	private static final long serialVersionUID = 918638989081830034L;
	private String applicationName;
	private long lastPingTime = System.currentTimeMillis();
    private long createdTime;
    private String ipAddress;
    private String clientHostName;    
    private String clientHardwareAddress;
    private String userName;
    private String vdbName;
    private int vdbVersion;
    private String sessionId;
    private String securityDomain;
    
    //server session state
    private transient VDBMetaData vdb;
    private transient SessionToken sessionToken;
    private transient LoginContext loginContext;
    private transient Object securityContext;
    private transient boolean embedded;

	@Override
	@ManagementProperty(description="Application associated with Session", readOnly=true)
	public String getApplicationName() {
		return this.applicationName;
	}
	
    public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}	
    
	@Override
	@ManagementProperty(description="When session created", readOnly=true)
	public long getCreatedTime() {
		return this.createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

	@Override
	@ManagementProperty(description="Host name from where the session created", readOnly=true)
	public String getClientHostName() {
		return this.clientHostName;
	}

	public void setClientHostName(String clientHostname) {
		this.clientHostName = clientHostname;
	}

	@Override
	@ManagementProperty(description="IP address of session client", readOnly=true)
	public String getIPAddress() {
		return this.ipAddress;
	}

	public void setIPAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	@Override
	@ManagementProperty(description="Last ping time", readOnly=true)
	public long getLastPingTime() {
		return this.lastPingTime;
	}

	public void setLastPingTime(long lastPingTime) {
		this.lastPingTime = lastPingTime;
	}

	@Override
	@ManagementProperty(description="Session ID", readOnly=true)
	public String getSessionId() {
		return this.sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	@ManagementProperty(description="User name associated with session", readOnly=true)
	public String getUserName() {
		return this.userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	@Override
	@ManagementProperty(description="VDB name associated with session", readOnly=true)
	public String getVDBName() {
		return this.vdbName;
	}

	public void setVDBName(String vdbName) {
		this.vdbName = vdbName;
	}

	@Override
	@ManagementProperty(description="VDB version name associated with session", readOnly=true)
	public int getVDBVersion() {
		return this.vdbVersion;
	}

	public void setVDBVersion(int vdbVersion) {
		this.vdbVersion = vdbVersion;
	}

	@Override
	@ManagementProperty(description="Security Domain that session logged into", readOnly=true)
	public String getSecurityDomain() {
		return this.securityDomain;
	}
	
	public void setSecurityDomain(String domain) {
		this.securityDomain = domain;
	}	
	
    @SuppressWarnings("nls")
	public String toString() {
    	StringBuilder str = new StringBuilder();
    	str.append("session: sessionid=").append(sessionId);
    	str.append("; userName=").append(userName);
    	str.append("; vdbName=").append(vdbName);
    	str.append("; vdbVersion=").append(vdbVersion);
    	str.append("; createdTime=").append(new Date(createdTime));
    	str.append("; applicationName=").append(applicationName);
    	str.append("; clientHostName=").append(clientHostName);
    	str.append("; clientHardwareAddress=").append(clientHardwareAddress);
    	str.append("; IPAddress=").append(ipAddress);
    	str.append("; securityDomain=").append(securityDomain); 
    	str.append("; lastPingTime=").append(new Date(lastPingTime));
    	return str.toString();
    }

	public VDBMetaData getVdb() {
		return vdb;
	}

	public void setVdb(VDBMetaData vdb) {
		this.vdb = vdb;
	}

	public SessionToken getSessionToken() {
		return sessionToken;
	}

	public void setSessionToken(SessionToken sessionToken) {
		this.sessionToken = sessionToken;
	}

	public LoginContext getLoginContext() {
		return loginContext;
	}

	public void setLoginContext(LoginContext loginContext) {
		this.loginContext = loginContext;
	}

	public Object getSecurityContext() {
		return securityContext;
	}

	public void setSecurityContext(Object securityContext) {
		this.securityContext = securityContext;
	}	
	
	public Subject getSubject() {
		return this.loginContext.getSubject();
	}
	
	public void setEmbedded(boolean embedded) {
		this.embedded = embedded;
	}

	public boolean isEmbedded() {
		return embedded;
	}
	
	@Override
	@ManagementProperty(description="Hardware address reported by the client", readOnly=true)
	public String getClientHardwareAddress() {
		return this.clientHardwareAddress;
	}
	
	public void setClientHardwareAddress(String clientHardwareAddress) {
		this.clientHardwareAddress = clientHardwareAddress;
	}
	
}
