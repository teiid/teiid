/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.resource.spi;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

public class BasicManagedConnection implements ManagedConnection {
    protected PrintWriter log;
    protected final Collection<ConnectionEventListener> listeners = new ArrayList<ConnectionEventListener>();
    private ResourceConnection physicalConnection;
    private final Set<WrappedConnection> handles = new HashSet<WrappedConnection>();

    public BasicManagedConnection(ResourceConnection connection) {
        this.physicalConnection = connection;
    }

    @Override
    public void associateConnection(Object handle) throws ResourceException {
        if (!(handle instanceof WrappedConnection)) {
            throw new ResourceException("Wrong connection supplied to assosiate"); //$NON-NLS-1$
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
        if (this.physicalConnection != null) {
            this.physicalConnection.cleanUp();
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
        ConnectionContext.setSubject(arg0);

        WrappedConnection wc = new WrappedConnection(this);
        synchronized(this.handles) {
            this.handles.add(wc);
        }
        return wc;
    }

    @Override
    public LocalTransaction getLocalTransaction() throws ResourceException {
        javax.resource.cci.LocalTransaction localTxn = this.physicalConnection.getLocalTransaction();
        if (localTxn == null) {
            return null;
        }
        return new LocalTransaction() {

            @Override
            public void rollback() throws ResourceException {
                localTxn.rollback();
            }

            @Override
            public void commit() throws ResourceException {
                localTxn.commit();
            }

            @Override
            public void begin() throws ResourceException {
                localTxn.begin();
            }
        };
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
    void connectionClosed(WrappedConnection wc) {

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
    }

   Connection getConnection() throws ResourceException {
      if (this.physicalConnection == null)
         throw new ResourceException("Connection has been destroyed!!!"); //$NON-NLS-1$
      return this.physicalConnection;
   }

   public boolean isValid() {
       if (this.physicalConnection == null) {
           return false;
       }
       return this.physicalConnection.isAlive();
   }
}
