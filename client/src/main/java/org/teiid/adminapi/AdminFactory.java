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

import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;
import org.teiid.net.ServerConnectionFactory;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.SocketServerConnectionFactory;


/** 
 * Singleton factory for class for creating Admin connections to the Teiid
 */
public class AdminFactory {
	
    private final class AdminProxy implements InvocationHandler {

    	private Admin admin;
    	private ServerConnection serverConnection;
    	private boolean closed;
    	
    	public AdminProxy(Properties p) throws ConnectionException, CommunicationException {
    		this.serverConnection = serverConnectionFactory.getConnection(p);
    		this.admin = serverConnection.getService(Admin.class);
		}
    	
    	private synchronized Admin getTarget() throws AdminComponentException {
    		if (closed) {
    			throw new AdminComponentException(JDBCPlugin.Util.getString("admin_conn_closed")); //$NON-NLS-1$
    		}
    		return admin;
    	}
    	
		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if (method.getName().equals("close")) { //$NON-NLS-1$
				close();
				return null;
			}
			if (!method.getDeclaringClass().equals(Admin.class)) {
				return method.invoke(this, args);
			}
			try {
				return method.invoke(getTarget(), args);
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}
		}
		
		public synchronized void close() {
			if (closed) {
				return;
			}
			this.closed = true;
			if (serverConnection != null) {
				serverConnection.close();
			}
		}		
    }

	public static final String DEFAULT_APPLICATION_NAME = "Admin"; //$NON-NLS-1$

    private static AdminFactory instance = new AdminFactory(SocketServerConnectionFactory.getInstance());
    
    private ServerConnectionFactory serverConnectionFactory;
    
    AdminFactory(ServerConnectionFactory connFactory) {
    	this.serverConnectionFactory = connFactory;
    }
    
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
     * @throws AdminException
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
     * @param applicationName
     * @return
     * @throws AdminException
     */
    public Admin createAdmin(String userName,
                                   char[] password,
                                   String serverURL,
                                   String applicationName) throws AdminException {
        
        if (userName == null || userName.trim().length() == 0) {
            throw new IllegalArgumentException(JDBCPlugin.Util.getString("invalid_parameter")); //$NON-NLS-1$
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
