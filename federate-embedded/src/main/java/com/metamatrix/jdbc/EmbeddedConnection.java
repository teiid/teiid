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

package com.metamatrix.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.admin.api.core.Admin;
import com.metamatrix.admin.api.embedded.EmbeddedAdmin;
import com.metamatrix.admin.api.exception.AdminComponentException;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.dqp.embedded.DQPEmbeddedManager;
import com.metamatrix.dqp.embedded.admin.DQPConfigAdminImpl;
import com.metamatrix.dqp.embedded.admin.DQPMonitoringAdminImpl;
import com.metamatrix.dqp.embedded.admin.DQPRuntimeStateAdminImpl;
import com.metamatrix.dqp.embedded.admin.DQPSecurityAdminImpl;

/** 
 * This class simple wrapper on top of MMConnection for the EmbeddedDriver and 
 * EmbeddedDataSource.
 * 
 * The specific reason for this class is to have a management API on the
 * a JDBC Connection.
 * 
 * @since 4.3
 */
public class EmbeddedConnection extends MMConnection {

    // constant value giving product name
    private final static String SERVER_NAME = "MetaMatrix Query"; //$NON-NLS-1$
    
    DQPEmbeddedManager manager = null;
    ConnectionListener listener = null;
    
    public static EmbeddedConnection newInstance(DQPEmbeddedManager manager, ServerConnection serverConn, Properties info, ConnectionListener listner) {
        return new EmbeddedConnection(manager, serverConn, info, listner);        
    }
    
    /**
     * ctor 
     */
    public EmbeddedConnection(DQPEmbeddedManager manager,
                              ServerConnection serverConn,
                              Properties info,
                              ConnectionListener listner) {
        super(serverConn, info, null);
        this.manager = manager;
        this.listener = listner;

        // tell the listener that connection has been created
        this.listener.connectionAdded(getConnectionId(), this);
    }

   /**
     * Get the Admin API interface, so that the caller can manager various aspects of configuring the DQP, like adding, deleteing
     * a VDB. Look at Java Docs for <code>Admin</code> all supported functionality.
     * 
     * @return retuns reference to API, never null.
     * @since 4.3
     */
    public Admin getAdminAPI() throws SQLException {
    
        InvocationHandler handler = new InvocationHandler() {
            Object[] implementors = {
                    new DQPConfigAdminImpl(manager),                    
                    new DQPMonitoringAdminImpl(manager), 
                    new DQPRuntimeStateAdminImpl(manager), 
                    new DQPSecurityAdminImpl(manager)
            };
            
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Exception ex = null;
                
                // We we perform any DQP functions check if the DQP is still alive
                if (!manager.isDQPAlive()) {
                    throw new AdminProcessingException(JDBCPlugin.Util.getString("EmbeddedConnection.DQP_shutDown")); //$NON-NLS-1$
                }
                
                // Since all the loading is done by the executing threads class loader, by defination we need to 
                // switch to our local non-delegating class loader each time the class enters into the dqp connection
                // boundary, however for simplicity sake we have only put this barrier, in the comm layer to isolate the
                // DQP code however the com.mm.jdbc.sql code will still use the calling class loader. By changing the 
                // class loader here, I am at atleast avoiding the classloading issues for the Admin code. - rreddy.
                ClassLoader callingClassLoader = Thread.currentThread().getContextClassLoader();
                try {
                    // Set the class loader to current class classloader so that the this classe's class loader gets used
                    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                    
                    for (int i = 0; i < implementors.length; i++) {
                        try {
                            return method.invoke(implementors[i], args);
                        } catch (IllegalArgumentException e) {
                            ex = e; // try all classes
                        } catch (IllegalAccessException e) {
                            throw e;
                        } catch (InvocationTargetException e) {
                            // Since we know all the admin methods throw the Admin Exceptions
                            // no need to wrap with undeclared exception.
                            Throwable target = e.getTargetException();
                            if (target instanceof AdminException) {
                                throw target;
                            }
                            throw new AdminComponentException(e);
                        }
                    }
                    if (ex != null) {
                        throw ex;
                    }                
                    return null;
                }
                finally {
                    Thread.currentThread().setContextClassLoader(callingClassLoader);
                }
            }            
        };
        return (EmbeddedAdmin) Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[] {EmbeddedAdmin.class}, handler);
    }

    /** 
     * @see com.metamatrix.jdbc.MMConnection#getDatabaseName()
     */
    @Override
    String getDatabaseName() {
        return SERVER_NAME;
    }

	@Override
	public BaseDriver getBaseDriver() {
		return new EmbeddedDriver();
	}

	@Override
	boolean isSameProcess(MMConnection conn) {
		return (conn instanceof EmbeddedConnection);
	}
}
