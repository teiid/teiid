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
package org.teiid.connector.basic;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.Referenceable;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.core.util.StringUtil;

public class WrappedConnector implements Connector, Referenceable, Serializable  {

	private static final long serialVersionUID = 5499157394014613035L;
	private Connector delegate;
	private ConnectionManager cm;	
	private BasicManagedConnectionFactory mcf;
	private Reference reference;
	ConnectorCapabilities caps;
	
	public WrappedConnector(Connector delegate, ConnectionManager cm, BasicManagedConnectionFactory mcf) {
		this.delegate = delegate;
		this.cm = cm;
		this.mcf = mcf;
	}
	
	@Override
	public void initialize(ConnectorEnvironment config) throws ConnectorException {
		this.delegate.initialize(config);
	}
	
	@Override
	public ConnectorCapabilities getCapabilities() throws ConnectorException {
		if (this.caps != null) {
			return this.caps;
		}
		
		// see if enhanced capabilities are available from the connector.
		this.caps = delegate.getCapabilities();
				
		// if not use the default capabilities specified in the configuration.
		if (this.caps == null) {
			try {
				Object o = ReflectionHelper.create(this.mcf.getCapabilitiesClass(), null, Thread.currentThread().getContextClassLoader());
				this.caps = (ConnectorCapabilities)o;
			} catch (MetaMatrixCoreException e) {
				throw new ConnectorException(e);
			} 
		}
		// capabilities overload
		ConnectorEnvironment env = getConnectorEnvironment();
		if (this.caps != null && env.getOverrideCapabilities() != null) {
			this.caps = (ConnectorCapabilities) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {ConnectorCapabilities.class}, new CapabilitesOverloader(this.caps, env.getOverrideCapabilities()));
		}
		return caps;
	}

	@Override
	public Connection getConnection() throws ConnectorException {
		try {
			return (Connection)cm.allocateConnection(mcf, new ConnectionRequestInfoWrapper(this.delegate));
		} catch (ResourceException e) {
			throw new ConnectorException(e);
		}
	}

	@Override
	public ConnectorEnvironment getConnectorEnvironment() {
		return this.delegate.getConnectorEnvironment();
	}

	@Override
	public void setReference(Reference arg0) {
		this.reference = arg0;
	}

	@Override
	public Reference getReference() throws NamingException {
		return this.reference;
	}

	
	/**
	 * Overloads the connector capabilities with one defined in the connector binding properties
	 */
    static final class CapabilitesOverloader implements InvocationHandler {
    	ConnectorCapabilities caps; 
    	Properties properties;
    	
    	CapabilitesOverloader(ConnectorCapabilities caps, Properties properties){
    		this.caps = caps;
    		this.properties = properties;
    	}
    	
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String value = this.properties.getProperty(method.getName());
			if (value == null || value.trim().length() == 0 || (args != null && args.length != 0)) {
				return method.invoke(this.caps, args);
			}
			return StringUtil.valueOf(value, method.getReturnType());
		}
	}	
}
