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

package org.teiid.transport;

import org.teiid.core.ComponentNotFoundException;
import org.teiid.deployers.VDBRepository;
import org.teiid.net.ConnectionException;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.SecurityHelper;


public interface ClientServiceRegistry {
	
	public enum Type {
		ODBC, JDBC
	}
	
	<T> T getClientService(Class<T> iface) throws ComponentNotFoundException;
	
	SecurityHelper getSecurityHelper();
	
	/**
	 * Provides the authentication type for pre-8.7 JDBC clients.
	 * 
	 * @return
	 */
	@Deprecated()
	AuthenticationType getAuthenticationType();
	
	void waitForFinished(String vdbName, int vdbVersion, int timeOutMillis) throws ConnectionException;
	
	ClassLoader getCallerClassloader();
	
	VDBRepository getVDBRepository();

}
