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

package com.metamatrix.platform.security.api.service;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.security.InvalidPrincipalException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;

/**
 * This interface represents the API to the MemberShip Service,
 * and it defines the functionality that is accessible to clients.
 */
public interface MembershipServiceInterface extends ApplicationService {
    public static String NAME = "MembershipService"; //$NON-NLS-1$
    
    /**
     * The environment property describing both the domain names and their required
     * order of search.  Domain names are assumed to be in preferred search order
     * and in the form "A,X,...,D" where A, X and D are domain names.
     * This property is required (there is no default).
     */
    public static final String DOMAIN_ORDER = "membership.DomainOrder"; //$NON-NLS-1$
    /**
     * The environment property name for the class that is to be used for the names of the domains.
     * This property is required (there is no default).
     */
    public static final String DOMAIN_NAME = "domainName"; //$NON-NLS-1$
    
    public static final String DEFAULT_ADMIN_USERNAME = "admin"; //$NON-NLS-1$
    public static final String DEFAULT_WSDL_USERNAME = CoreConstants.DEFAULT_ANON_USERNAME;
    
    public static final String ADMIN_PASSWORD = "membership.superUserPassword"; //$NON-NLS-1$
    public static final String ADMIN_USERNAME = "membership.superUser"; //$NON-NLS-1$
    public static final String DOMAIN_ACTIVE = "activate"; //$NON-NLS-1$
    public static final String ADMIN_HOSTS = "membership.allowedHosts"; //$NON-NLS-1$
    public static final String SECURITY_ENABLED = "membership.enabled"; //$NON-NLS-1$
    
    public static final String DOMAIN_PROPERTIES = "propertiesFile"; //$NON-NLS-1$

	public static final String AT = "@"; //$NON-NLS-1$

    /**
     * Authenticate a user with the specified username and credential
     * for use with the specified application. The application name may also
     * be used by the Membership Service to determine the appropriate authentication
     * mechanism.
     * @param username the username that is to be authenticated
     * @param credential the credential provided by the user that is to be used
     * to authenticate the user for the principal name
     * @param trustePayload
     * @param applicationName the name of the application for which the user
     * is authenticating
     * @return true if the specified credentials properly authenticates for
     * the application the user with the specified username and application
     * @throws MetaMatrixSecurityException if there is an error within this
     * service or during communicating with the underlying service provider
     */
    AuthenticationToken authenticateUser(String username, Credentials credential, Serializable trustePayload, String applicationName)
    throws MembershipServiceException;

    /**
     * Obtain the principal object that is representative of the user with the specified username.
     * 
     * all names should be domain qualified.
     */
    MetaMatrixPrincipal getPrincipal(MetaMatrixPrincipalName principal)
    throws MembershipServiceException, InvalidPrincipalException;
    
    /**
     * Obtain the collection of groups to which this user belongs
     * 
     * The username should be fully qualified
     */
    Set<String> getGroupsForUser(String username)
    throws MembershipServiceException, InvalidPrincipalException;
    
    /**
     * Obtain the collection of group names. 
     */
    Set<String> getGroupNames() throws MembershipServiceException;

    List<String> getDomainNames() throws MembershipServiceException;

    Set<String> getGroupsForDomain(String domainName) throws MembershipServiceException;
    
    boolean isSuperUser(String username) throws MembershipServiceException;
    
    boolean isSecurityEnabled() throws MembershipServiceException;  
}
