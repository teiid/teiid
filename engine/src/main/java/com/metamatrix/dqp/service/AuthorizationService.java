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

package com.metamatrix.dqp.service;

import java.util.Collection;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.AuthorizationMgmtException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.query.eval.SecurityFunctionEvaluator;

/**
 * This service provides a means to check whether a connection is authorized to access
 * various data resources.
 */
public interface AuthorizationService extends ApplicationService, SecurityFunctionEvaluator {

    public static final int ACTION_READ = 0;
    public static final int ACTION_CREATE = 1;
    public static final int ACTION_UPDATE = 2;
    public static final int ACTION_DELETE = 3;

    public static final int CONTEXT_QUERY = 0;
    public static final int CONTEXT_INSERT = 1;
    public static final int CONTEXT_UPDATE = 2;
    public static final int CONTEXT_DELETE = 3;
    public static final int CONTEXT_PROCEDURE = 4;
    
    public static final String DEFAULT_WSDL_USERNAME = CoreConstants.DEFAULT_ANON_USERNAME;
    
    public static final String ENTITELEMENTS_ENABLED = "entitlements.enabled"; //$NON-NLS-1$
    public static final String ADMIN_ROLES_FILE = "entitlements.adminRolesFile"; //$NON-NLS-1$
    
    /**
     * Determine which of a set of resources a connection does not have permission to
     * perform the specified action.
     * @param connectionID Connection ID identifying the connection (and thus the user credentials)
     * @param action Action connection wishes to perform
     * @param resources Resources the connection wishes to perform the action on, Collection of String
     * @param context Auditing context
     * @return Collection Subset of resources
     * @throws MetaMatrixComponentException If an error occurs in the service while checking resources
     */
    Collection getInaccessibleResources(String connectionID, int action, Collection resources, int context) throws MetaMatrixComponentException;

    /**
     * Determine whether entitlements checking is enabled on the server.
     * @return <code>true</code> iff server-side entitlements checking is enabled.
     */
    boolean checkingEntitlements();
    
    boolean isCallerInRole(SessionToken session, String roleName ) throws AuthorizationMgmtException;
    
    
    /**
     * Returns a Collection of String names of MetaMatrix roles to which the
     * given principal is assigned.
     * @param caller the session token of the principal that is attempting to access the roles.
     * @param principal <code>MetaMatrixPrincipalName</code> for which roles are sought
     * @return The <code>Collection</code> of role names the principal is assigned.
     * @throws InvalidSessionException if the administrative session is invalid
     * @throws MetaMatrixSecurityException if there is a problem internally with the MembershipService
     * @throws AuthorizationException if administrator does not have the authority to see the requested information
     * @throws ComponentNotFoundException if a component required by this method could not be found within the server
     */
    Collection<String> getRoleNamesForPrincipal(MetaMatrixPrincipalName principal)
    throws InvalidSessionException, AuthorizationException, AuthorizationMgmtException;
    
    /**
     * Returns a <code>Collection</code> of <code>AuthorizationPolicy</code>s
     * that have <code>AuthorizationPermission</code>s in the given <code>AuthorizationRealm</code>.<br>
     * <strong>NOTE:</strong> It is the responsibility of the caller to determine
     * which of the <code>AuthorizationPolicy</code>'s <code>AuthorizationPermission</code>s
     * are actually in the given <code>AuthorizationRealm</code>.  The <code>AuthorizationPolicy</code>
     * may span <code>AuthorizationRealm</code>s.
     * @param caller The session token of the principal that is attempting to retrieve the policies.
     * @param realm The realm in which to search for <code>AuthorizationPermission</code>s.
     * @return The collection of <code>AuthorizationPolicy</code>s that have permissions
     * in the given realm - possibly empty but never null.
     * @throws AuthorizationException if administrator does not have the authority to perform the action.
     * @throws AuthorizationMgmtException if an error occurs in the Authorization store.
     */
    Collection<AuthorizationPolicy> getPoliciesInRealm(AuthorizationRealm realm)
    throws AuthorizationException, AuthorizationMgmtException;    
    
    void updatePoliciesInRealm(AuthorizationRealm realm, Collection<AuthorizationPolicy> policies) throws AuthorizationMgmtException;
}
