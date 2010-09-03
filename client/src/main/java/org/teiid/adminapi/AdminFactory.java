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

package org.teiid.adminapi;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import org.teiid.client.security.LogonException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.NetPlugin;
import org.teiid.net.ServerConnection;
import org.teiid.net.ServerConnectionFactory;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.SocketServerConnectionFactory;


/** 
 * Singleton factory for ServerAdmins.
 * @since 4.3
 */
public class AdminFactory {
	
    private static final int DEFAULT_BOUNCE_WAIT = 2000;
        
    private final class AdminProxy implements InvocationHandler {

    	private Admin target;
    	private ServerConnection registry;
    	private Properties p;
    	private boolean closed;
    	
    	public AdminProxy(Properties p) throws ConnectionException, CommunicationException {
    		this.p = p;
    		this.registry = serverConnectionFactory.getConnection(p);
    		this.target = registry.getService(Admin.class);
		}
    	
    	private synchronized Admin getTarget() throws AdminComponentException {
    		if (closed) {
    			throw new AdminComponentException(NetPlugin.Util.getString("ERR.014.001.0001")); //$NON-NLS-1$
    		}
    		return target;
    	}
    	
		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if (method.getName().equals("close")) { //$NON-NLS-1$
				close();
				return null;
			}
			try {
				return method.invoke(getTarget(), args);
			} catch (InvocationTargetException e) {
				if (ExceptionUtil.getExceptionOfType(e, CommunicationException.class) != null) {
					this.target = null;
					if (method.getName().endsWith("restart")) { //$NON-NLS-1$
						bounceSystem(true);
						return null;
					}
				}
				throw e.getTargetException();
			}
		}
		
		public synchronized void close() {
			if (closed) {
				return;
			}
			this.closed = true;
			if (registry != null) {
				registry.close();
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
				throw new TeiidRuntimeException(e);
			}
			
			//we'll wait 30 seconds for the server to come back up
        	for (int i = 0; i < 15; i++) {
        		try {
        			getTarget().getProcesses(AdminObject.WILDCARD);
        			return;
        		} catch (Exception e) {
                    //reestablish a connection and retry
                    try {
						Thread.sleep(bounceWait);
					} catch (InterruptedException ex) {
						throw new TeiidRuntimeException(ex);
					}                                        
        		}
        	}
		}
    }

	public static final String DEFAULT_APPLICATION_NAME = "Admin"; //$NON-NLS-1$

    /**Singleton instance*/
    private static AdminFactory instance = new AdminFactory(SocketServerConnectionFactory.getInstance(), DEFAULT_BOUNCE_WAIT);
    
    private ServerConnectionFactory serverConnectionFactory;
    private int bounceWait;
    
    AdminFactory(ServerConnectionFactory connFactory, int bounceWait) {
    	this.serverConnectionFactory = connFactory;
    	this.bounceWait = bounceWait;
    }
    
    /**Get the singleton instance*/
    public static AdminFactory getInstance() {
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
            throw new IllegalArgumentException(NetPlugin.Util.getString("ERR.014.001.0099")); //$NON-NLS-1$
        }
        
    	final Properties p = new Properties();
    	p.setProperty(TeiidURL.CONNECTION.APP_NAME, applicationName);
    	p.setProperty(TeiidURL.CONNECTION.USER_NAME, userName);
        if (password != null) {
        	p.setProperty(TeiidURL.CONNECTION.PASSWORD, new String(password));
        }
    	p.setProperty(TeiidURL.CONNECTION.SERVER_URL, serverURL);
    	return createAdmin(p);
    }

	public Admin createAdmin(Properties p) throws AdminException {
		p = PropertiesUtils.clone(p);
		p.remove(TeiidURL.JDBC.VDB_NAME);
		p.remove(TeiidURL.JDBC.VDB_VERSION);
    	p.setProperty(TeiidURL.CONNECTION.ADMIN, Boolean.TRUE.toString());
    	
		try {
			Admin serverAdmin = (Admin)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] { Admin.class }, new AdminProxy(p));
			return serverAdmin;
		} catch (ConnectionException e) {				
			throw new AdminComponentException(e);
		} catch (CommunicationException e) {
			throw new AdminComponentException(e);
		}
    }
    
}
