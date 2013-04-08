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
package org.teiid.resource.spi;

import java.io.Serializable;
import java.util.HashMap;

import javax.resource.spi.ManagedConnectionFactory;

public class TeiidSecurityCredential implements Serializable {
	private static final long serialVersionUID = 7725328159246858176L;

	public enum SecurityHandler {WSPOLICY, WSS4J};

	private HashMap<String, Object> requestProps = new HashMap<String, Object>();
	private HashMap<String, Object> responseProps = new HashMap<String, Object>();


	private ManagedConnectionFactory mcf;
	private SecurityHandler securityHandler = SecurityHandler.WSS4J;

	/**
	 * Gets the target ManagedConnectionFactory for which the user name and
	 * password has been set by the application server. A ManagedConnection-
	 * Factory uses this field to find out whether PasswordCredential should be
	 * used by it for sign-on to the target EIS instance.
	 *
	 * @return ManagedConnectionFactory instance for which user name and
	 *         password have been specified
	 **/
	public ManagedConnectionFactory getManagedConnectionFactory() {
		return this.mcf;
	}

	/**
	 * Sets the target ManagedConenctionFactory instance for which the user name
	 * and password has been set by the application server.
	 *
	 * @param mcf
	 *            ManagedConnectionFactory instance for which user name and
	 *            password have been specified
	 **/
	public void setManagedConnectionFactory(ManagedConnectionFactory mcf) {
		this.mcf = mcf;
	}

	public HashMap<String, Object> getRequestPropterties(){
		return this.requestProps;
	}

	public HashMap<String, Object> getResponsePropterties(){
		return this.responseProps;
	}

	public SecurityHandler getSecurityHandler() {
		return this.securityHandler;
	}

	public void setSecurityHandler(SecurityHandler securityHandler) {
		this.securityHandler = securityHandler;
	}
}
