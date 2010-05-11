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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.SecureRandom;
import java.util.Arrays;

import org.teiid.core.util.Base64;


/**
 * This class is an immutable identifier for a unique session that also
 * maintains the name of the principal for that session.
 * 
 * Since this class can be used to authenticate a user, it must be secure in 
 * transit if sent to the client.  Also it should only be sent to the client 
 * who creates the session.
 */
public class SessionToken implements Externalizable {
	public final static long serialVersionUID = -2853708320435636107L;

	private static final SecureRandom random = new SecureRandom();
	
	/** The session ID */
	private String sessionID;
	private String userName;
	private byte[] secret = new byte[16];
	
	public SessionToken() {
	}

	/**
	 * Used by tests to control the session id
	 * 
	 * @param id
	 * @param userName
	 */
	public SessionToken(long id, String userName) {
		this.sessionID = Long.toString(id);
		this.userName = userName;
	}	

	/**
	 * The primary constructor that specifies userName
	 * 
	 * @param userName
	 * 		(String) the userName for this session
	 */	
	public SessionToken(String userName) {
		byte[] bytes = new byte[9]; //9 bytes fits evenly into base64 and should be sufficiently cluster unique
		random.nextBytes(bytes);
		this.sessionID = Base64.encodeBytes(bytes);
		this.userName = userName;
		random.nextBytes(secret);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof SessionToken)) {
			return false;
		}
		SessionToken other = (SessionToken)obj;
		return userName.equals(other.userName) 
			&& sessionID.equals(other.sessionID)
			&& Arrays.equals(secret, other.secret);
	}
	
	/**
	 * Returns unique session identifier
	 * 
	 * @return the session ID
	 */
	public String getSessionID() {
		return this.sessionID;
	}

	/**
	 * Get the principal name for this session's user.
	 * 
	 * @return the user name
	 */
	public String getUsername() {
		return this.userName;
	}

	/**
	 * Returns a string representing the current state of the object.
	 */
	public String toString() {
		return "SessionToken[" + getUsername() + "," + this.sessionID + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		secret = (byte[])in.readObject();
		sessionID = (String)in.readObject();
		userName = (String)in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(secret);
		out.writeObject(sessionID);
		out.writeObject(userName);
	}

}
