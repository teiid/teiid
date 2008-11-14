/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.io.Serializable;

import com.metamatrix.core.util.StringUtil;

/**
 * This class maintains MM WSDL Url information. The toString() method is overridden to provide the WSDL Url.
 */
public class WSDLUrl implements Serializable{

	private String scheme;
	private String host;
	private String port;
	private String vdbName;
	private String version;
	private static final String APP_NAME = "metamatrix-soap"; //$NON-NLS-1$
	private static final String WSDL = "wsdl"; //$NON-NLS-1$
	private static final String DELIM = "/"; //$NON-NLS-1$
	private static final String DOUBLE_DELIM = "//"; //$NON-NLS-1$
	private static final String COLON = ":"; //$NON-NLS-1$

	/**
	 * @param host
	 * @param port
	 * @param protocol
	 * @param name
	 * @param version
	 */
	public WSDLUrl( String host,
	                String port,
	                String protocol,
	                String name,
	                String version ) {
		this.host = host;
		this.port = port;
		this.scheme = protocol;
		this.vdbName = name;
		this.version = version;
	}

	/**
	 * @return
	 */
	public String getProtocol() {
		return scheme;
	}

	/**
	 * @param protocol
	 */
	public void setProtocol( String protocol ) {
		this.scheme = protocol;
	}

	/**
	 * @return
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @param host
	 */
	public void setHost( String host ) {
		this.host = host;
	}

	/**
	 * @return
	 */
	public String getPort() {
		return port;
	}

	/**
	 * @param port
	 */
	public void setPort( String port ) {
		this.port = port;
	}

	/**
	 * @return
	 */
	public String getVdbName() {
		return vdbName;
	}

	/**
	 * @param vdbName
	 */
	public void setVdbName( String vdbName ) {
		this.vdbName = vdbName;
	}

	/**
	 * @return
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * @param version
	 */
	public void setVersion( String version ) {
		this.version = version;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer url = new StringBuffer();
		url.append(scheme).append(COLON).append(DOUBLE_DELIM).append(host);
		url.append(COLON).append(port).append(DELIM).append(APP_NAME);
		url.append(DELIM).append(WSDL).append(DELIM).append(vdbName);

		if (version != null && !StringUtil.Constants.EMPTY_STRING.equals(version)) {
			url.append(DELIM).append(version);
		}

		return url.toString();
	}
}
