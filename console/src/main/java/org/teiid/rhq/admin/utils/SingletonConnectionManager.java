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
package org.teiid.rhq.admin.utils;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.teiid.rhq.admin.ConnectionMgr;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;


public class SingletonConnectionManager {

	private static final Log log = LogFactory.getLog(SingletonConnectionManager.class);
    
    private static final String ENTERPRISE_CONNECTION_MGR="org.teiid.rhq.enterprise.EnterpriseConnectionMgr"; //$NON-NLS-1$
    private static final String EMBEDDED_CONNECTION_MGR="org.teiid.rhq.embedded.EmbeddedConnectionMgr"; //$NON-NLS-1$

	private static SingletonConnectionManager instance = null;
	private ConnectionMgr connmgr;
	private ClassLoader loader;
	private boolean initialized = false;


    private SingletonConnectionManager() {	
         
         
	}

	public synchronized static SingletonConnectionManager getInstance() {

    		if (instance == null) {
                   instance = new SingletonConnectionManager();
                   instance.setupConnectionMgr();
    		}
            
		return instance;
	}	
    
	public Set getInstallationSystemKeys() {
		return connmgr.getInstallationSystemKeys();
	}

	public Connection getConnection(String key) throws ConnectionException {
    	return  connmgr.getConnection(key);
    }
	

	public void initialize(Properties props) {
		if (connmgr != null && initialized) {
			// re-establish the connectionfactory and pool
			shutdown();
			setupConnectionMgr();
		}
		initializeConnectionMgr(props);
		
	}

	public Map getServerInstallations() {
		return connmgr.getServerInstallations();
	}

	public boolean hasServersDefined() {
		return connmgr.hasServersDefined();
	}

	public void shutdown() {
		connmgr.shutdown();
		connmgr = null;
	}

	
	
	private void setupConnectionMgr() {
		this.loader = this.getClass().getClassLoader();
    	ConnectionMgr mgr = null;
    	Class clzz = null;
    	// first try enterprise version
    	try { 
    		clzz = Class.forName(ENTERPRISE_CONNECTION_MGR, true, this.loader); 
    	} catch (ClassNotFoundException noent) {
    		
    		// no try the embedded connection pool version
        	try { 
        		clzz = Class.forName(EMBEDDED_CONNECTION_MGR, true, this.loader); 
        	} catch (ClassNotFoundException noemb) {
        		
        	}
    	}
    	
    	if (clzz == null) {
    		throw new InvalidPluginConfigurationException("System Error: cannot load either enterprise or embedded connection mgr");
    	}
    	
        try {
			mgr = (ConnectionMgr) clzz.newInstance();
		} catch (Exception e) {
			throw new InvalidPluginConfigurationException(e);
		}              

    	this.connmgr = mgr;
    	
    }
	
	private void initializeConnectionMgr(Properties props) {
            
		connmgr.initialize(props, this.loader);
		this.initialized = true;
    	
    }
	
	public boolean isEmbedded() {
    	try { 
    		Class.forName(EMBEDDED_CONNECTION_MGR);
    		return true;
    	} catch (ClassNotFoundException noent) {
    		return false;    		
    	}    	
    }

}
