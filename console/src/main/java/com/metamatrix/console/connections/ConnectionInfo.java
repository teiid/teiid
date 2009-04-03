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

package com.metamatrix.console.connections;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.api.exception.ComponentCommunicationException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.client.ServerAdminFactory;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

public class ConnectionInfo {
    private String url;
	private String user;
    private char[] password;
    private String applicationName;
	private ServerConnection connection;
    private List hosts;
    private String connectedHost;
    private List ports;
    private String connectedPort;
    private MMURL mmurl;
    private ServerAdmin serverAdmin;
    
	public ConnectionInfo(String serverURL,
                          String user,
                          char[] password,
                          String applicationName) {
		super();
        this.url = serverURL;
		this.user = user;
		this.password = password;
        this.applicationName = applicationName;
        mmurl = new MMURL(serverURL);
        
        
        List infos = mmurl.getHostInfo();
        hosts = new ArrayList(infos.size());
        ports = new ArrayList(infos.size());
        for (Iterator it=infos.iterator(); it.hasNext();) {
            HostInfo hi = (HostInfo) it.next();
            hosts.add(hi.getHostName().toUpperCase());
            ports.add(String.valueOf(hi.getPortNumber()));
        }
        
        // for now taking the first host and port in the list
        // until the connection can tell us which one it actually used
        connectedHost = (String) hosts.get(0);
        connectedPort = (String) ports.get(0);
    }
	
	public void setUser(String user) {
		this.user = user;
	}
	
	public String getUser() {
		return user;
	}
	
	public String getURL() {
		return url;
	}
	
    public String getHost() {
        return connectedHost;
    }
    
    public List getHosts() {
        return hosts;
    }
    
    public String getPort() {
        return connectedPort;
    }
    
    public List getPorts() {
        return ports;
    }
    
    public char[] getPassword() {
        return this.password;
    }

    public String getApplicationName() {
        return this.applicationName;
    }

    
    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public void setPassword(char[] password) {
        this.password = password;
    }

    public boolean isConnectedHost(String hostName) {
        // if there are multiple hosts in the url
        // there currently no way to determine which one that
        // was connected to.        
        if (hosts.size() == 1) {
            return connectedHost.equalsIgnoreCase(hostName); 
        }
        return false;
    }
    
    public boolean isConnectedHost(String hostName, String port) {
        if (isConnectedHost(hostName)) {
            return ports.contains(port);
        }
        
        return false;
    }    
    
    public String getClusterName() {
    	return connection.getLogonResult().getClusterName();
    }
    
    /**
     * Get a ServerIConnection.
     * @param create If true, create the connection if it doesn't already exist.  
     * @return
     * @since 4.3
     */
    public ServerConnection getServerConnection(boolean create) {
        if (! create) {
            return connection;
        }
        
	    if (connection != null && isOpen()) {
	        return connection;
	    }
	    try {
            // Case 4793 - changed to true - network latency issues causing problems with reconnect
            connection = relogin();

            return connection;
        } catch (MetaMatrixComponentException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch(Exception e) {
            e.printStackTrace();
            throw new MetaMatrixRuntimeException(e);
        }
	}
    
    
    public MetaMatrixSessionID getSessionID() {
        return getServerConnection().getLogonResult().getSessionID();
    }
    
    private boolean isOpen() {
        if (connection == null) {
            return false;
        }

        return connection.isOpen();
    }
    
    
    
    /**
     * Get an adminAPIConnection.
     * Create it if it doesn't already exist. 
     * @return
     * @since 4.3
     */
	public ServerConnection getServerConnection() {
	    return getServerConnection(true);
	}
    
	public int hashCode() {
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getURL());
        hc = HashCodeUtil.hashCode(hc, getUser());
        return hc;
	}
	
	public boolean equals(Object obj) {
		boolean same;
		if (obj == null) {
			same = false;
		} else if (obj == this) {
			same = true;
		} else if (!(obj instanceof ConnectionInfo)) {
			same = false;
		} else {
			ConnectionInfo conn = (ConnectionInfo)obj;
			same = (url.equals(conn.getURL()) && user.equals(conn.getUser()));
		}
		return same;
	}
	
	public String toString () {
		String str = getURL() + '[' + getUser() + ']';
		return str;
	}
    
    /**
     * Create a new ServerConnection. 
     * @return
     * @throws ConnectionException if login failed.
     * @throws CommunicationException if the server was unreachable.
     * @throws LogonException 
     * @since 4.3
     */
    public ServerConnection login( ) throws ConnectionException, CommunicationException, LogonException {
        if (user == null || user.trim().length() == 0) {
            throw new IllegalArgumentException(AdminPlugin.Util.getString(AdminMessages.ADMIN_0099));
        }
        if (password == null || password.length == 0) {
            throw new IllegalArgumentException(AdminPlugin.Util.getString(AdminMessages.ADMIN_0100));
        }
        
        Properties properties = new Properties();
        properties.setProperty(MMURL.CONNECTION.USER_NAME, user);
        properties.setProperty(MMURL.CONNECTION.PASSWORD, new String(password));
        properties.setProperty(MMURL.CONNECTION.APP_NAME, applicationName);
        properties.setProperty(MMURL.CONNECTION.PRODUCT_NAME, MetaMatrixProductNames.Platform.PRODUCT_NAME);
        properties.setProperty(MMURL.CONNECTION.SERVER_URL, mmurl.getAppServerURL());
        connection = SocketServerConnectionFactory.getInstance().createConnection(properties);
        ModelManager.clearServices(this);

        String postLoginName = connection.getLogonResult().getUserName();
        if(postLoginName!=null && !postLoginName.equalsIgnoreCase(user)) {
        	user = postLoginName;
        }
        return connection;
    }

    public ServerConnection relogin() throws MetaMatrixComponentException {

        close();
        
        Exception e = null;
        connection = null;
        try {              
            connection = login();
            boolean initSucceeded = ModelManager.init(this);    

            if (initSucceeded) {
                return connection;
            }                
        } catch (Exception ex) {
            e = ex;
        }
        // If we're here we've retried the maximum number of times
        String msg = "Lost communication with Server."; //$NON-NLS-1$
        if (e instanceof LogonException) {
        	msg = "Current credentials are no longer valid.  This connection should be closed."; //$NON-NLS-1$
        }
        ComponentCommunicationException cce = new ComponentCommunicationException(e, msg);
        throw cce;        
    }
        
    
    /**
     * Close the underlying connection. 
     * 
     * @since 4.3
     */
    public void close() {
        if (connection != null) {
        	connection.shutdown();
        }
        if (this.serverAdmin != null) {
        	this.serverAdmin.close();
        }
    }
    
    
    /**
	 * Get a ServerAdmin object.  Make sure to call ServerAdmin.close() when you're done with it.
     * TODO: cache this?  Need to make sure it gets closed when the console exits. 
     * @throws CommunicationException 
	 */
    public synchronized ServerAdmin getServerAdmin() throws AdminException, LogonException, CommunicationException {
    	if (this.serverAdmin == null) {
    		this.serverAdmin = ServerAdminFactory.getInstance().createAdmin(user, password, url, applicationName);
    	}
        return this.serverAdmin;
    }
   
}
