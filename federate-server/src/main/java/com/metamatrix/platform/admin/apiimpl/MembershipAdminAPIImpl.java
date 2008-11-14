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

package com.metamatrix.platform.admin.apiimpl;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.InvalidUserException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.common.util.MultipleRequestConfirmation;
import com.metamatrix.platform.admin.api.MembershipAdminAPI;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.util.PlatformProxyHelper;

public class MembershipAdminAPIImpl extends SubSystemAdminAPIImpl implements MembershipAdminAPI {

    // Auth svc proxy
    private MembershipServiceInterface membAdmin;
    private static MembershipAdminAPI membershipAdminAPI;

    /**
     * ctor
     */
    private MembershipAdminAPIImpl() throws MetaMatrixComponentException {
        
        membAdmin = PlatformProxyHelper.getMembershipServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
    }

    public synchronized static MembershipAdminAPI getInstance() throws MetaMatrixComponentException {
        if (membershipAdminAPI == null) {
            membershipAdminAPI = new MembershipAdminAPIImpl();
        }
        return membershipAdminAPI;
    }

    public synchronized List getDomainNames( ) throws AuthorizationException,
                                                      InvalidSessionException,
                                                      MetaMatrixComponentException,
                                                      MetaMatrixSecurityException {
    	// Validate caller's session
    	// SessionToken token =
    	AdminAPIHelper.validateSession(getSessionID());

    	// Any administrator may call this read-only method - no need to validate role
    	try {
    	    return membAdmin.getDomainNames();
    	}catch(RemoteException e) {
    	    throw new MetaMatrixComponentException(e);
    	}
    }

    public synchronized Set getGroupsForDomain(String domainName) throws AuthorizationException,
    														InvalidSessionException,
    														MetaMatrixComponentException,
    														MetaMatrixSecurityException {
    	// Validate caller's session
    	// SessionToken token =
    	AdminAPIHelper.validateSession(getSessionID());
    	// Any administrator may call this read-only method - no need to validate role
    	try {
    	    return membAdmin.getGroupsForDomain(domainName);
        }catch(RemoteException e) {
            throw new MetaMatrixComponentException(e);
        }    	    
    }
    
    /**
     * Authenticate the given user / credentials as a valid system user using the given payload and application name
     * @param username - user to authenticate
     * @param credential - credentials to use when validating user
     * @param trustedpayload - payload to use when validating user
     * @param applicationName - applicationName to use when validating user
     * @return Will return either a SuccessfulAuthenticationToken or an UnsuccessfulAuthenticationToken 
     * @see com.metamatrix.platform.admin.api.MembershipAdminAPI#authenticateUser(java.lang.String, com.metamatrix.platform.security.api.Credentials, java.io.Serializable, java.lang.String)
     *
     */
    public Serializable authenticateUser(String username, Credentials credential, Serializable trustePayload, String applicationName) throws MetaMatrixComponentException, MembershipServiceException {
        try {
            return membAdmin.authenticateUser(username, credential, trustePayload, applicationName);
        }catch(RemoteException e) {
            throw new MetaMatrixComponentException(e);
        }        
    }

    public synchronized MetaMatrixPrincipal getUserPrincipal(String principalName) throws AuthorizationException,
                                                                                  InvalidSessionException,
                                                                                  MetaMatrixComponentException,
                                                                                  MetaMatrixSecurityException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
        	return membAdmin.getPrincipal(new MetaMatrixPrincipalName(principalName, MetaMatrixPrincipal.TYPE_USER));
        }catch(RemoteException e) {
            throw new MetaMatrixComponentException(e);
        }        
    }

    public synchronized MultipleRequestConfirmation getUserPrincipals(Collection userNames) throws AuthorizationException,
                                                                                           InvalidSessionException,
                                                                                           MetaMatrixComponentException,
                                                                                           MetaMatrixSecurityException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        MultipleRequestConfirmation result = new MultipleRequestConfirmation();
        Collection principals = new HashSet(userNames.size());

        Iterator iter = userNames.iterator();
        String userName = null;
        MetaMatrixPrincipal principal = null;
        while (iter.hasNext()) {
            userName = (String)iter.next();
            try {
                principal = membAdmin.getPrincipal(new MetaMatrixPrincipalName(userName, MetaMatrixPrincipal.TYPE_USER));
                principals.add(principal);
            } catch (InvalidUserException e) {
                result.addFailure(userName, e);
            } catch (MetaMatrixSecurityException e) {
                result.addFailure(userName, e);
            } catch(RemoteException e) {
                result.addFailure(userName, e);
            }
        }
        result.setResult(principals);
        return result;
    }

    /**
     * Add the given set of principals to the given role.
     * 
     * @param principals
     *            Set of <code>MetaMatrixPrincipalName</code>s to which to add.
     * @param roleName
     *            The name of the role to which to add the principals.
     * @throws InvalidSessionException
     *             if the administrative session is invalid
     * @throws AuthorizationException
     *             if admninistrator does not have the authority to perform the requested operation.
     * @throws MetaMatrixComponentException
     *             if this service has trouble communicating.
     */
    public synchronized MultipleRequestConfirmation getGroupPrincipals(Collection groupNames) throws AuthorizationException,
                                                                                             InvalidSessionException,
                                                                                             MetaMatrixComponentException,
                                                                                             MetaMatrixSecurityException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role

        MultipleRequestConfirmation result = new MultipleRequestConfirmation();
        Collection principals = new HashSet(groupNames.size());

        Iterator iter = groupNames.iterator();
        String groupName = null;
        MetaMatrixPrincipal principal = null;
        while (iter.hasNext()) {
            groupName = (String)iter.next();
            try {
                principal = membAdmin.getPrincipal(new MetaMatrixPrincipalName(groupName, MetaMatrixPrincipal.TYPE_GROUP));
                principals.add(principal);
            } catch (InvalidUserException e) {
                result.addFailure(groupName, e);
            } catch (MetaMatrixSecurityException e) {
                result.addFailure(groupName, e);
            } catch(RemoteException e) {
                result.addFailure(groupName, e);
            }
        }
        result.setResult(principals);
        return result;
    }
    
    public synchronized Collection getGroupPrincipalNames() throws AuthorizationException,
                                                               InvalidSessionException,
                                                               MetaMatrixComponentException,
                                                               MetaMatrixSecurityException {
        // Validate caller's session
        // SessionToken token =
        AdminAPIHelper.validateSession(getSessionID());
        // Any administrator may call this read-only method - no need to validate role
        try {
        return membAdmin.getGroupNames();
        }catch(RemoteException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

}
