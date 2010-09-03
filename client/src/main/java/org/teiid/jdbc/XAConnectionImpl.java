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

package org.teiid.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.net.CommunicationException;
import org.teiid.net.ServerConnection;
import org.teiid.net.socket.SingleInstanceCommunicationException;

/**
 * Implementation of XAConnection.
 */
public class XAConnectionImpl implements XAConnection{
	
    interface ConnectionSource {

        ConnectionImpl createConnection() throws SQLException;
        
    }
    
    private final class CloseInterceptor implements
                                        InvocationHandler {
        
        private ConnectionImpl proxiedConnection;
        
        CloseInterceptor(ConnectionImpl connection) {
            this.proxiedConnection = connection; 
        }

        public Object invoke(Object proxy,
                             Method method,
                             Object[] args) throws Throwable {
            if ("close".equals(method.getName())) {  //$NON-NLS-1$
                this.proxiedConnection.recycleConnection();
                XAConnectionImpl.this.notifyListener(null);
                return null;
            }
            
            try {
				return method.invoke(this.proxiedConnection, args);
			} catch (InvocationTargetException e) {
				Exception ex = ExceptionUtil.getExceptionOfType(e, InvalidSessionException.class);
				if (ex == null) {
					ex = ExceptionUtil.getExceptionOfType(e, CommunicationException.class);
					if (ex instanceof SingleInstanceCommunicationException) {
						ServerConnection sc = proxiedConnection.getServerConnection();
						if (!sc.isOpen(ServerConnection.PING_INTERVAL)) {
							ex = null;
						}
					}
				}
				if (ex != null) {
					SQLException se = null;
					if (e.getCause() instanceof SQLException) {
						se = (SQLException)e.getCause();
					} else {
						se = TeiidSQLException.create(e.getCause());
					}
					notifyListener(se);
				} 
				throw e.getTargetException();
			}
        }
    }

    private HashSet listeners;
	private XAResource resource;
	private ConnectionImpl connection;
	private ConnectionSource cs;
		
    private boolean isClosed;
        
    public static XAConnectionImpl newInstance (ConnectionSource cs){
        return new XAConnectionImpl(cs);
    }
    
	public XAConnectionImpl(ConnectionSource cs){
	    this.cs = cs;
	}
		
	public Connection getConnection() throws SQLException{
        ConnectionImpl conn = getConnectionImpl();
		
		Connection result = (Connection)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {Connection.class}, new CloseInterceptor(conn));
		
		return result;
	}
	
	ConnectionImpl getConnectionImpl() throws SQLException {
	    if(isClosed){
            throw new SQLException(JDBCPlugin.Util.getString("MMXAConnection.connection_is_closed")); //$NON-NLS-1$
        }
        
        if(connection == null){
            try{
                connection = cs.createConnection();
            }catch(SQLException e){                
                notifyListener(e);
                throw e;
            }       
        }
        
        return connection;
	}
	    
	public synchronized void addConnectionEventListener(ConnectionEventListener listener){
		if(listeners == null){
			listeners = new HashSet();
		}
		this.listeners.add(listener);
	}
	
	public synchronized void removeConnectionEventListener(ConnectionEventListener listener){
		if(listeners == null){
			return;
		}
		this.listeners.remove(listener);
	}
	
	public XAResource getXAResource() throws SQLException{
		if(resource == null){
			resource = XAResourceImpl.newInstance(this);
		}
		return resource;
	}
	
	public void close()throws SQLException{		
		if(connection != null && !connection.isClosed()){
			connection.close();			
		}
        isClosed = true;
	}
	
	/**
	 * Notify listeners, if there is any, about the connection status.
	 * If e is null, the connection is properly closed.
	 * @param e
	 */
	protected synchronized void notifyListener(SQLException e){
		if(listeners != null && !listeners.isEmpty()){
			Iterator iter = listeners.iterator();
			while(iter.hasNext()){
				ConnectionEventListener listener = (ConnectionEventListener)iter.next();
				if(e == null){
					//no exception
					listener.connectionClosed(new ConnectionEvent(this));
				}else{
					//exception occurred
					listener.connectionErrorOccurred(new ConnectionEvent(this, e));	
				}
			}
		}	
	}

	public void addStatementEventListener(StatementEventListener arg0) {
	}

	public void removeStatementEventListener(StatementEventListener arg0) {
	}
}
