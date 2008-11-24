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

package com.metamatrix.common.comm.platform.client;

import java.lang.reflect.Proxy;
import java.util.Properties;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.exception.AdminComponentException;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.MixinProxy;

/** 
 * Singleton factory for ServerAdmins.
 * @since 4.3
 */
public class ServerAdminFactory {
	
    private static final int BOUNCE_WAIT = 1000;
    
    private final class ClientAdminState {
		private ServerAdmin remoteProxy;
		private ServerConnection registry;

		public ClientAdminState(ServerAdmin serverAdmin, ServerConnection registry) {
			this.remoteProxy = serverAdmin;
			this.registry = registry;
		}
		
		public void close() {
			registry.shutdown();
		}
		
		public void bounceSystem(boolean waitUntilDone) throws AdminException {
			remoteProxy.bounceSystem(waitUntilDone);
			
	        if (waitUntilDone) {
	            while (true) {
	                try {
	                	//check that we can connect to the server
	                    remoteProxy.getSystem();
	                    break; // must be back up
	                } catch (Exception e) {
	                    //ignore
	                } finally {
	                    //reestablish a connection and retry
	                    try {
							Thread.sleep(BOUNCE_WAIT);
						} catch (InterruptedException e) {
							throw new MetaMatrixRuntimeException(e);
						}                                        
	                }
	            }
	        }
		}
	}

	public static final String DEFAULT_APPLICATION_NAME = "Admin"; //$NON-NLS-1$

    /**Singleton instance*/
    private static ServerAdminFactory instance = null;
    
    private ServerAdminFactory() {        
    }
    
    /**Get the singleton instance*/
    public synchronized static ServerAdminFactory getInstance() {
        if (instance == null) {
            instance = new ServerAdminFactory();           
        }
        
        return instance;
    }
    
    
    /**
     * Creates a ServerAdmin with the specified connection properties. 
     * Uses the DEFAULT_APPLICATION_NAME as the application name.
     * @param userName
     * @param password
     * @param serverURL
     * @return
     * @throws LogonException
     * @throws AdminException
     * @throws CommunicationException 
     * @throws LogonException 
     * @since 4.3
     */
    public ServerAdmin createAdmin(String userName,
                             char[] password,
                             String serverURL) throws AdminException {
        
        return createAdmin(userName, password, serverURL, DEFAULT_APPLICATION_NAME);
        
    }
    
    /**
     * Creates a ServerAdmin with the specified connection properties. 
     * @param userName
     * @param password
     * @param serverURL
     * @return
     * @throws LogonException
     * @throws AdminException
     * @throws CommunicationException 
     * @throws LogonException 
     * @since 4.3
     */
    public ServerAdmin createAdmin(String userName,
                                   char[] password,
                                   String serverURL,
                                   String applicationName) throws AdminException {
        
        if (userName == null || userName.trim().length() == 0) {
            throw new IllegalArgumentException(AdminPlugin.Util.getString("ERR.014.001.0099")); //$NON-NLS-1$
        }
        if (password == null || password.length == 0) {
            throw new IllegalArgumentException(AdminPlugin.Util.getString("ERR.014.001.00100")); //$NON-NLS-1$
        }
        
        return createAdminProxy(userName, password, serverURL, applicationName);
    }
    
    private ServerAdmin createAdminProxy(final String userName,
                                         final char[] password,
                                         final String serverURL,
                                         final String applicationName) throws AdminException {
    	final Properties p = new Properties();
    	p.setProperty(MMURL_Properties.JDBC.APP_NAME, applicationName);
    	p.setProperty(MMURL_Properties.JDBC.USER_NAME, userName);
    	p.setProperty(MMURL_Properties.JDBC.PASSWORD, new String(password));
    	p.setProperty(MMURL_Properties.SERVER.SERVER_URL, serverURL);
    	return createAdmin(p);
    }

	public ServerAdmin createAdmin(final Properties p)
			throws AdminComponentException, AdminException {
		p.setProperty(MMURL_Properties.CONNECTION.PRODUCT_NAME, MetaMatrixProductNames.Platform.PRODUCT_NAME);
    	
    	ServerConnection registry;
		try {
			registry = SocketServerConnectionFactory.getInstance().createConnection(p);
		} catch (CommunicationException e) {
			throw new AdminComponentException(e.getMessage());
		} catch (ConnectionException e) {
			throw new AdminComponentException(e.getMessage());
		}
    	
    	ServerAdmin serverAdmin = registry.getService(ServerAdmin.class);
    	
    	ClientAdminState clientAdminState = new ClientAdminState(serverAdmin, registry);
    	
    	//mixin local behavior
    	serverAdmin = (ServerAdmin) Proxy.newProxyInstance(Thread.currentThread()
				.getContextClassLoader(), new Class[] { ServerAdmin.class },
				new MixinProxy(new Object[] { clientAdminState, serverAdmin }));
    	
        //make a method call, to test that we are connected 
    	serverAdmin.getSystem();
        
        return serverAdmin;
    }
    
}
