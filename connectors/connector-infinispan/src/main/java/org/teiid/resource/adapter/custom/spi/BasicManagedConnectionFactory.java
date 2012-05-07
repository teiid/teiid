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
package org.teiid.resource.adapter.custom.spi;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.resource.spi.ValidatingManagedConnectionFactory;
import javax.security.auth.Subject;



public abstract class BasicManagedConnectionFactory implements ManagedConnectionFactory, ResourceAdapterAssociation, ValidatingManagedConnectionFactory {

	private static final long serialVersionUID = -7302713800883776790L;
	private PrintWriter log;
	private BasicResourceAdapter ra;
	private BasicConnectionFactory cf;
	
	@Override
	public abstract BasicConnectionFactory createConnectionFactory() throws ResourceException;

	@Override
	public Object createConnectionFactory(ConnectionManager cm) throws ResourceException {
		this.cf = createConnectionFactory();
		return this.cf;
		// return new WrappedConnectionFactory(this.cf, cm, this);
	}

	@Override
	public ManagedConnection createManagedConnection(Subject arg0, ConnectionRequestInfo arg1) throws ResourceException {
//		Assertion.isNotNull(this.cf);
		ConnectionContext.setSubject(arg0);
		
		BasicConnection connection = null;
		if (arg1 instanceof ConnectionRequestInfoWrapper) {
			connection = this.cf.getConnection(((ConnectionRequestInfoWrapper)arg1).cs);
		}
		else {
			connection = this.cf.getConnection();
		}
		ConnectionContext.setSubject(null);
		return new BasicManagedConnection(connection);
	}

	@Override
	public PrintWriter getLogWriter() throws ResourceException {
		return this.log;
	}

	@Override
	public ManagedConnection matchManagedConnections(Set arg0, Subject arg1, ConnectionRequestInfo arg2) throws ResourceException {
		return (ManagedConnection)arg0.iterator().next();
	}

	@Override
	public void setLogWriter(PrintWriter arg0) throws ResourceException {
		this.log = arg0;
	}

	@Override
	public ResourceAdapter getResourceAdapter() {
		return this.ra;
	}

	@Override
	public void setResourceAdapter(ResourceAdapter arg0) throws ResourceException {
		this.ra = (BasicResourceAdapter)arg0;
	}
	
    public static <T> T getInstance(Class<T> expectedType, String className, Collection ctorObjs, Class defaultClass) throws ResourceException {
    	try {
	    	if (className == null) {
	    		if (defaultClass == null) {
	    			throw new ResourceException("Neither class name or default class specified to create an instance"); //$NON-NLS-1$
	    		}
	    		return expectedType.cast(defaultClass.newInstance());
	    	}
	    	return expectedType.cast(ReflectionHelper.create(className, ctorObjs, Thread.currentThread().getContextClassLoader()));
		} catch (IllegalAccessException e) {
			throw new ResourceException(e);
		} catch(InstantiationException e) {
			throw new ResourceException(e);
		} catch (Exception e) {
			throw new ResourceException(e);
		}   	
    }

	@Override
	public Set<BasicManagedConnection> getInvalidConnections(Set arg0) throws ResourceException {
		HashSet<BasicManagedConnection> result = new HashSet<BasicManagedConnection>();
		for (Object object : arg0) {
			if (object instanceof BasicManagedConnection) {
				BasicManagedConnection bmc = (BasicManagedConnection)object;
				if (!bmc.isValid()) {
					result.add(bmc);
				}
			}
		}
		return result;
	}
}
