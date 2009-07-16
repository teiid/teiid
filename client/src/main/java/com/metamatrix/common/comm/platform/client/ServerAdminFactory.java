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

package com.metamatrix.common.comm.platform.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;

/** 
 * Singleton factory for ServerAdmins.
 * @since 4.3
 */
public class ServerAdminFactory {
	
    private static final int DEFAULT_BOUNCE_WAIT = 2000;
        
    private final class ReconnectingProxy implements InvocationHandler {

    	private Admin target;
    	private ServerConnection registry;
    	private Properties p;
    	private boolean closed;
    	
    	public ReconnectingProxy(Properties p) {
    		this.p = p;
		}
    	
    	private synchronized Admin getTarget() throws AdminComponentException, CommunicationException {
    		if (closed) {
    			throw new AdminComponentException(CommPlatformPlugin.Util.getString("ERR.014.001.0001")); //$NON-NLS-1$
    		}
    		if (target != null && registry.isOpen()) {
    			return target;
    		}
    		try {
    			registry = serverConnectionFactory.createConnection(p);
    		} catch (ConnectionException e) {
    			throw new AdminComponentException(e.getMessage());
    		}
    		target = registry.getService(Admin.class);
    		return target;
    	}
    	
		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if (method.getName().equals("close")) { //$NON-NLS-1$
				close();
				return null;
			}
			Throwable t = null;
			for (int i = 0; i < 3; i++) {
				try {
					return method.invoke(getTarget(), args);
				} catch (InvocationTargetException e) {
					if (method.getName().endsWith("restart") && ExceptionUtil.getExceptionOfType(e, CommunicationException.class) != null) { //$NON-NLS-1$
						bounceSystem(true);
						return null;
					}
					throw e.getTargetException();
				} catch (CommunicationException e) {
					t = e;
				}
			}
			throw t;
		}
		
		public synchronized void close() {
			if (closed) {
				return;
			}
			this.closed = true;
			if (registry != null) {
				registry.shutdown();
			}
		}
		
		public void bounceSystem(boolean waitUntilDone) {
	        if (!waitUntilDone) {
	        	return;
	        }
        	//we'll wait 2 seconds for the server to come up
        	try {
				Thread.sleep(bounceWait);
			} catch (InterruptedException e) {
				throw new MetaMatrixRuntimeException(e);
			}
			//we'll wait 30 seconds for the server to come back up
        	for (int i = 0; i < 15; i++) {
        		try {
        			getTarget().getSystem();
        			return;
        		} catch (Exception e) {
                    //reestablish a connection and retry
                    try {
						Thread.sleep(bounceWait);
					} catch (InterruptedException ex) {
						throw new MetaMatrixRuntimeException(ex);
					}                                        
        		}
        	}
		}
    }

	public static final String DEFAULT_APPLICATION_NAME = "Admin"; //$NON-NLS-1$

    /**Singleton instance*/
    private static ServerAdminFactory instance = new ServerAdminFactory(SocketServerConnectionFactory.getInstance(), DEFAULT_BOUNCE_WAIT);
    
    private ServerConnectionFactory serverConnectionFactory;
    private int bounceWait;
    
    ServerAdminFactory(ServerConnectionFactory connFactory, int bounceWait) {
    	this.serverConnectionFactory = connFactory;
    	this.bounceWait = bounceWait;
    }
    
    /**Get the singleton instance*/
    public static ServerAdminFactory getInstance() {
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
    public Admin createAdmin(String userName,
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
    public Admin createAdmin(String userName,
                                   char[] password,
                                   String serverURL,
                                   String applicationName) throws AdminException {
        
        if (userName == null || userName.trim().length() == 0) {
            throw new IllegalArgumentException(AdminPlugin.Util.getString("ERR.014.001.0099")); //$NON-NLS-1$
        }
        
    	final Properties p = new Properties();
    	p.setProperty(MMURL.CONNECTION.APP_NAME, applicationName);
    	p.setProperty(MMURL.CONNECTION.USER_NAME, userName);
        if (password != null) {
        	p.setProperty(MMURL.CONNECTION.PASSWORD, new String(password));
        }
    	p.setProperty(MMURL.CONNECTION.SERVER_URL, serverURL);
    	return createAdmin(p);
    }

	public Admin createAdmin(Properties p) {
		p = PropertiesUtils.clone(p);
		p.remove(MMURL.JDBC.VDB_NAME);
		p.remove(MMURL.JDBC.VDB_VERSION);
    	p.setProperty(MMURL.CONNECTION.AUTO_FAILOVER, Boolean.TRUE.toString());
		Admin serverAdmin = (Admin)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] { Admin.class }, new ReconnectingProxy(p));
    	
       return serverAdmin;
    }
    
}
