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

package com.metamatrix.platform.util;

import java.util.Properties;

import com.google.inject.Inject;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.proxy.ProxyManager;
import com.metamatrix.platform.service.proxy.ServiceProxyProperties;
import com.metamatrix.server.connector.service.ConnectorServiceInterface;
import com.metamatrix.server.query.service.QueryServiceInterface;


public class PlatformProxyHelper {

	@Inject
	static ProxyManager proxy; // in ServerGuiceModule, there is explicit call to inject this.
	
    public static final String RANDOM = "RANDOM"; //$NON-NLS-1$
    public static final String ROUND_ROBIN = "ROUND_ROBIN"; //$NON-NLS-1$
    public static final String ROUND_ROBIN_LOCAL = "ROUND_ROBIN_LOCAL"; //$NON-NLS-1$
    public static final String RANDOM_LOCAL = "RANDOM_LOCAL"; //$NON-NLS-1$
    
    public final static String SESSION_SERVICE_PROXY_CLASS         = SessionServiceInterface.class.getName();
    public final static String AUTHORIZATION_SERVICE_PROXY_CLASS   = AuthorizationServiceInterface.class.getName();
    public final static String MEMBERSHIP_SERVICE_PROXY_CLASS      = MembershipServiceInterface.class.getName();
    public final static String CONFIGURATION_SERVICE_PROXY_CLASS   = ConfigurationServiceInterface.class.getName();
    public final static String QUERY_SERVICE_PROXY_CLASS 		   = QueryServiceInterface.class.getName();
    public final static String CONNECTOR_SERVICE_PROXY_CLASS 	   = ConnectorServiceInterface.class.getName();

    public static final SessionServiceInterface getSessionServiceProxy(String policyType) throws ServiceException {

        Properties props = new Properties();
        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, SESSION_SERVICE_PROXY_CLASS);
        props.put(ServiceProxyProperties.SERVICE_SELECTION_POLICY_NAME, policyType);
        
        return (SessionServiceInterface) proxy.findOrCreateProxy(SessionServiceInterface.NAME, props);
    }

    public static final AuthorizationServiceInterface getAuthorizationServiceProxy(String policyType) throws ServiceException {

        Properties props = new Properties();
        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, AUTHORIZATION_SERVICE_PROXY_CLASS);
        props.put(ServiceProxyProperties.SERVICE_SELECTION_POLICY_NAME, policyType);

        return (AuthorizationServiceInterface) proxy.findOrCreateProxy(AuthorizationServiceInterface.NAME, props);
    }

    public static final MembershipServiceInterface getMembershipServiceProxy(String policyType) throws ServiceException {

        Properties props = new Properties();
        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, MEMBERSHIP_SERVICE_PROXY_CLASS);
        props.put(ServiceProxyProperties.SERVICE_SELECTION_POLICY_NAME, policyType);

        return (MembershipServiceInterface) proxy.findOrCreateProxy(MembershipServiceInterface.NAME, props);
    }

    public static final ConfigurationServiceInterface getConfigurationServiceProxy(String policyType) throws ServiceException {

        Properties props = new Properties();
        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, CONFIGURATION_SERVICE_PROXY_CLASS);
        props.put(ServiceProxyProperties.SERVICE_SELECTION_POLICY_NAME, policyType);

        return (ConfigurationServiceInterface) proxy.findOrCreateProxy(ConfigurationServiceInterface.NAME, props);
    }
    

    public static final QueryServiceInterface getQueryServiceProxy(String policyType) throws ServiceException {

        Properties props = new Properties();
        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, QUERY_SERVICE_PROXY_CLASS);
        props.put(ServiceProxyProperties.SERVICE_SELECTION_POLICY_NAME, policyType);
        props.put(ServiceProxyProperties.SERVICE_MULTIPLE_DELEGATION, Boolean.TRUE.toString());

        return (QueryServiceInterface) proxy.findOrCreateProxy(QueryServiceInterface.SERVICE_NAME, props);
    }

    public static final ConnectorServiceInterface getConnectorServiceProxy(String routingID, String policyType) throws ServiceException {

        Properties props = new Properties();
        props.put(ServiceProxyProperties.SERVICE_PROXY_CLASS_NAME, CONNECTOR_SERVICE_PROXY_CLASS);
        props.put(ServiceProxyProperties.SERVICE_SELECTION_POLICY_NAME, policyType);
        props.put(ServiceProxyProperties.SERVICE_SELECTION_STICKY, Boolean.TRUE.toString());
        return (ConnectorServiceInterface) proxy.findOrCreateProxy(routingID, props);
    }      
}
