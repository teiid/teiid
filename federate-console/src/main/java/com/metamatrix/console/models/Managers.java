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

package com.metamatrix.console.models;

public class Managers {
	private GroupsManager groupsManager = null;
    private SessionManager sessionManager;
    private QueryManager queryManager;
    private EntitlementManager entitlementManager;
    private PoolManager poolManager;
    private SummaryManager summaryManager;
    private ServerLogManager serverLogManager;
    private PropertiesManager propertiesManager;
    private VdbManager vdbManager;
    private ConfigurationManager configurationManager;
    private ConnectorManager connectorManager;
    private AuthenticationProviderManager authenticationProviderManager;
    private RuntimeMgmtManager runtimeMgmtManager;
    private ExtensionSourceManager extensionSourceManager;
    
    public Managers(GroupsManager groupsManager, SessionManager sessionManager,
    		QueryManager queryManager, EntitlementManager entitlementManager, 
    		PoolManager poolManager,
    		SummaryManager summaryManager, 
    		ServerLogManager serverLogManager,
    		PropertiesManager propertiesManager, VdbManager vdbManager,
    		ConfigurationManager configurationManager,
    		ConnectorManager connectorManager, AuthenticationProviderManager authManager,
    		RuntimeMgmtManager runtimeMgmtManager,
    		ExtensionSourceManager extensionSourceManager) {
    	super();
    	this.groupsManager = groupsManager;
    	this.sessionManager = sessionManager;
    	this.queryManager = queryManager;
    	this.entitlementManager = entitlementManager;
    	this.poolManager = poolManager;
    	this.summaryManager = summaryManager;
    	this.serverLogManager = serverLogManager;
    	this.propertiesManager = propertiesManager;
    	this.vdbManager = vdbManager;
    	this.configurationManager = configurationManager;
    	this.connectorManager = connectorManager;
    	this.authenticationProviderManager = authManager;
    	this.runtimeMgmtManager = runtimeMgmtManager;
    	this.extensionSourceManager = extensionSourceManager;
    }
    
    public GroupsManager getGroupsManager() {
    	return groupsManager;
    }
    
    public SessionManager getSessionManager() {
    	return sessionManager;
    }
    
    public QueryManager getQueryManager() {
    	return queryManager;
    }
    
    public EntitlementManager getEntitlementManager() {
    	return entitlementManager;
    }
    
    public PoolManager getPoolManager() {
    	return poolManager;
    }
    
    public SummaryManager getSummaryManager() {
    	return summaryManager;
    }

    public ServerLogManager getServerLogManager() {
    	return serverLogManager;
    }
    
    public PropertiesManager getPropertiesManager() {
    	return propertiesManager;
    }
    
    public VdbManager getVDBManager() {
    	return vdbManager;
    }
    
    public ConfigurationManager getConfigurationManager() {
    	return configurationManager;
    }
    
    public ConnectorManager getConnectorManager() {
    	return connectorManager;
    }
    
    public AuthenticationProviderManager getAuthenticationProviderManager() {
    	return authenticationProviderManager;
    }
    
    public RuntimeMgmtManager getRuntimeMgmtManager() {
    	return runtimeMgmtManager;
    }
    
    public ExtensionSourceManager getExtensionSourceManager() {
    	return extensionSourceManager;
    }
}
