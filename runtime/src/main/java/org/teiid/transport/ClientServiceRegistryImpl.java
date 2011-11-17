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

import java.util.HashMap;

import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.SecurityHelper;


public class ClientServiceRegistryImpl implements ClientServiceRegistry {
	
	public static class ClientService {
		private Object instance;
		private String loggingContext;
		private ReflectionHelper reflectionHelper;
		
		public ClientService(Object instance, String loggingContext,
				ReflectionHelper reflectionHelper) {
			this.instance = instance;
			this.loggingContext = loggingContext;
			this.reflectionHelper = reflectionHelper;
		}
		
		public Object getInstance() {
			return instance;
		}
		public String getLoggingContext() {
			return loggingContext;
		}
		public ReflectionHelper getReflectionHelper() {
			return reflectionHelper;
		}
	}
	
    private HashMap<String, ClientService> clientServices = new HashMap<String, ClientService>();
    private SecurityHelper securityHelper;
    private Type type = Type.JDBC;
    private AuthenticationType authenticationType = AuthenticationType.CLEARTEXT;
    
    public ClientServiceRegistryImpl() {
    	
    }
    
    public ClientServiceRegistryImpl(Type type) {
    	this.type = type;
	}
    
    public void setAuthenticationType(AuthenticationType authenticationType) {
		this.authenticationType = authenticationType;
	}

    public <T> T getClientService(Class<T> iface) throws ComponentNotFoundException {
    	ClientService cs = getClientService(iface.getName());
    	return iface.cast(cs.getInstance());
    }
    
	public ClientService getClientService(String iface) throws ComponentNotFoundException {
		ClientService cs = clientServices.get(iface);
		if (cs == null) {
			throw new ComponentNotFoundException(RuntimePlugin.Util.getString("ServerWorkItem.Component_Not_Found", type, iface)); //$NON-NLS-1$
		}
		return cs;
	}

	public <T> void registerClientService(Class<T> iface, T instance, String loggingContext) {
		this.clientServices.put(iface.getName(), new ClientService(instance, loggingContext, new ReflectionHelper(iface)));
	}	
	
	@Override
	public SecurityHelper getSecurityHelper() {
		return this.securityHelper;
	}
	
	public void setSecurityHelper(SecurityHelper securityHelper) {
		this.securityHelper = securityHelper;
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return authenticationType;
	}
		
}
