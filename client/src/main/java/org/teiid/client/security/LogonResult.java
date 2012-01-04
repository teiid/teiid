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

package org.teiid.client.security;

import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.teiid.core.util.ExternalizeUtil;



/**
 * Dataholder for the result of <code>ILogon.logon()</code>.
 * Contains a sessionID
 * 
 * Analogous to the server side SessionToken
 */
public class LogonResult implements Externalizable {
        
	private static final long serialVersionUID = 4481443514871448269L;
	private TimeZone timeZone = TimeZone.getDefault();
    private String clusterName;
    private SessionToken sessionToken;
    private String vdbName;
    private int vdbVersion;
    private Map<Object, Object> addtionalProperties;

	public LogonResult() {
	}
    
    public LogonResult(SessionToken token, String vdbName, int vdbVersion, String clusterName) {
		this.clusterName = clusterName;
		this.sessionToken = token;
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
	}

	/**
     * Get the sessionID. 
     * @return
     * @since 4.3
     */
    public String getSessionID() {
        return this.sessionToken.getSessionID();
    }

	public TimeZone getTimeZone() {
		return timeZone;
	}
	

	public String getUserName() {
		return this.sessionToken.getUsername();
	}

	public String getClusterName() {
		return clusterName;
	}
	
	public SessionToken getSessionToken() {
		return sessionToken;
	}

	public String getVdbName() {
		return vdbName;
	}

	public int getVdbVersion() {
		return vdbVersion;
	}
	
    public Object getProperty(String key) {
		if (this.addtionalProperties == null) {
			return null;
		}
		return addtionalProperties.get(key);
    }

	public void addProperty(String key, Object value) {
		if (this.addtionalProperties == null) {
			this.addtionalProperties = new HashMap<Object, Object>();
		}
		this.addtionalProperties.put(key, value);
	}	
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		vdbName = (String)in.readObject();
		sessionToken = (SessionToken)in.readObject();
		timeZone = (TimeZone)in.readObject();
		clusterName = (String)in.readObject();
		vdbVersion = in.readInt();
		try {
			addtionalProperties = ExternalizeUtil.readMap(in);
		} catch (EOFException e) {
			
		} catch (OptionalDataException e) {
			
		}
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(vdbName);
		out.writeObject(sessionToken);
		out.writeObject(timeZone);
		out.writeObject(clusterName);
		out.writeInt(vdbVersion);
		ExternalizeUtil.writeMap(out, addtionalProperties);
	}
    
}
