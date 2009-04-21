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

package com.metamatrix.jdbc;

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
//## JDBC4.0-begin ##
import javax.sql.StatementEventListener;
//## JDBC4.0-end ##
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

/**
 * Implementation of XAConnection.
 */
public class MMXAConnection implements XAConnection{
	
    interface ConnectionSource {

        MMConnection createConnection() throws SQLException;
        
    }
    
    private final class CloseInterceptor implements
                                        InvocationHandler {
        
        private MMConnection proxiedConnection;
        
        CloseInterceptor(MMConnection connection) {
            this.proxiedConnection = connection; 
        }

        public Object invoke(Object proxy,
                             Method method,
                             Object[] args) throws Throwable {
            if ("close".equals(method.getName())) {  //$NON-NLS-1$
                this.proxiedConnection.recycleConnection();
                MMXAConnection.this.notifyListener(null);
                return null;
            }
            
            try {
				return method.invoke(this.proxiedConnection, args);
			} catch (InvocationTargetException e) {
				//TODO: notify listeners about critical errors
				throw e.getTargetException();
			}
        }
    }

    private HashSet listeners;
	private XAResource resource;
	private MMConnection connection;
	private ConnectionSource cs;
		
    private boolean isClosed;
        
    public static MMXAConnection newInstance (ConnectionSource cs){
        return new MMXAConnection(cs);
    }
    
	public MMXAConnection(ConnectionSource cs){
	    this.cs = cs;
	}
		
	public Connection getConnection() throws SQLException{
        MMConnection conn = getMMConnection();
		
		Connection result = (Connection)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {com.metamatrix.jdbc.api.Connection.class}, new CloseInterceptor(conn));
		
		return result;
	}
	
	MMConnection getMMConnection() throws SQLException {
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
			resource = MMXAResource.newInstance(this);
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

	//## JDBC4.0-begin ##
	public void addStatementEventListener(StatementEventListener arg0) {
	}

	public void removeStatementEventListener(StatementEventListener arg0) {
	}
	//## JDBC4.0-end ##
}
