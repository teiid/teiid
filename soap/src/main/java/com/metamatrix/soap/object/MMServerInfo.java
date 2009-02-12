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

package com.metamatrix.soap.object;

/**
 * Contains information related to a MetaMatrix server instance used
 * for discovering MetaMatrix Web Service VDB instances.
 */
public class MMServerInfo {

	/**
	 * The server name value of the Metamatrix server(s) instance
	 */
	private String server;
	/**
	 * The protocol in use for the Metamatrix server instance
	 */
	private String protocol;

	/**
	 * @param protocol
	 * @param server
	 */
	public MMServerInfo( String protocol,
	                     String server ) {
		this.protocol = protocol;
		this.server = server;
	}

	/**
	 * @return
	 */
	public String getProtocol() {
		return protocol;
	}

	/**
	 * @param protocol
	 */
	public void setProtocol( String protocol ) {
		this.protocol = protocol;
	}

	/**
	 * @return
	 */
	public String getServer() {
		return server;
	}

	/**
	 * @param server
	 */
	public void setServer( String server ) {
		this.server = server;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer serverUrl = new StringBuffer();
		serverUrl.append(protocol).append("://").append(server); //$NON-NLS-1$
		return serverUrl.toString();
	}
}
