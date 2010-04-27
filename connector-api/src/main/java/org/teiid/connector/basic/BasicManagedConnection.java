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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectionContext;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorException;

public class BasicManagedConnection implements ManagedConnection {
	protected PrintWriter log;
	protected final Collection<ConnectionEventListener> listeners = new ArrayList<ConnectionEventListener>();
	private BasicManagedConnectionFactory mcf;
	private Connection physicalConnection;
	private final Set<WrappedConnection> handles = new HashSet<WrappedConnection>();
	private Connector cf;
	
	public BasicManagedConnection(Connector connectionFactory, BasicManagedConnectionFactory mcf) throws ResourceException  {
		this.cf = connectionFactory;
		this.physicalConnection = this.cf.getConnection();
		this.mcf = mcf;
	}

	@Override
	public void associateConnection(Object handle) throws ResourceException {
		if (!(handle instanceof WrappedConnection)) {
			throw new ConnectorException("Wrong connection supplied to assosiate");
		}
		((WrappedConnection)handle).setManagedConnection(this);
		synchronized (this.handles) {
			this.handles.add((WrappedConnection)handle);
		}
	}

	@Override
	public void cleanup() throws ResourceException {
		synchronized (this.handles) {
			for (WrappedConnection wc:this.handles) {
				wc.setManagedConnection(null);
			}
			handles.clear();
		}
		ConnectionContext.setSubject(null);
	}

	@Override
	public void destroy() throws ResourceException {
		cleanup();
		
		this.physicalConnection.close();
		this.physicalConnection = null;
	}
	
	@Override
	public ManagedConnectionMetaData getMetaData() throws ResourceException {
		return null;
	}
	
	@Override
	public Object getConnection(Subject arg0, ConnectionRequestInfo arg1) throws ResourceException {
		if(!(arg1 instanceof ConnectionRequestInfoWrapper)) {
			throw new ConnectorException("Un recognized Connection Request Info object received");
		}
		ConnectionContext.setSubject(arg0);
		if (this.physicalConnection == null) {
			this.physicalConnection = this.cf.getConnection();
		}
		
		WrappedConnection wc = new WrappedConnection(this, mcf); 
		synchronized(this.handles) {
			this.handles.add(wc);
		}
		return wc; 
	}

	@Override
	public LocalTransaction getLocalTransaction() throws ResourceException {
		return this.physicalConnection.getLocalTransaction();
	}

	@Override
	public XAResource getXAResource() throws ResourceException {
		return this.physicalConnection.getXAResource();
	}
	
	@Override
	public void addConnectionEventListener(ConnectionEventListener arg0) {
		synchronized (this.listeners) {
			this.listeners.add(arg0);
		}
	}	

	@Override
	public void removeConnectionEventListener(ConnectionEventListener arg0) {
		synchronized (this.listeners) {
			this.listeners.remove(arg0);
		}
	}

	@Override
	public void setLogWriter(PrintWriter arg0) throws ResourceException {
		this.log = arg0;
	}
	
	@Override
	public PrintWriter getLogWriter() throws ResourceException {
		return this.log;
	}

	// called by the wrapped connection to notify the close of the connection.
	void connectionClosed(WrappedConnection wc) throws ConnectorException {
		
		synchronized (this.handles) {
			handles.remove(wc);
		}
		
		ConnectionEvent ce = new ConnectionEvent(this, ConnectionEvent.CONNECTION_CLOSED);
		ce.setConnectionHandle(wc);
		
		ArrayList<ConnectionEventListener> copy = null;
		synchronized (this.listeners) {
			copy = new ArrayList<ConnectionEventListener>(this.listeners);
		}
		
		for(ConnectionEventListener l: copy) {
			l.connectionClosed(ce);
		}
		
		// check if connector is a facade for another; in which case release it to the pool
		if (this.mcf.getSourceJNDIName() != null) {
			synchronized(this.physicalConnection) {
				this.physicalConnection.close();
				this.physicalConnection = null;
			}
		}
	}
	
	public boolean isValid() throws ConnectorException {
		return this.physicalConnection.isAlive();
	}
	
   Connection getConnection() throws ConnectorException {
      if (this.physicalConnection == null)
         throw new ConnectorException("Connection has been destroyed!!!");
      return this.physicalConnection;
   }	
}
