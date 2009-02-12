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

package com.metamatrix.platform.security.authorization.spi;

import java.util.HashSet;
import java.util.Set;

import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPermissionFactory;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.AuthorizationResource;
import com.metamatrix.platform.security.api.DataAccessResource;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;
import com.metamatrix.platform.security.util.RolePermissionFactory;

/**
 * @since	  3.0
 */
public final class AuthorizationTestUtil {

    final static String METABASE_REALM_NAME = "MetaBase"; //$NON-NLS-1$

    // Matches values in com.metamatrix.platform.security.api.MetaMatrixPrincipal
    public static final int PRINCIPAL_TYPE_USER        = 0;
    public static final int PRINCIPAL_TYPE_GROUP       = 1;
    public static final int PRINCIPAL_TYPE_ADMIN       = 2;

    public static final AuthorizationPermissionFactory METABASE_FACTORY = new RolePermissionFactory();

    private static final int crudInt = StandardAuthorizationActions.DATA_CREATE_VALUE |
                                       StandardAuthorizationActions.DATA_READ_VALUE   |
                                       StandardAuthorizationActions.DATA_UPDATE_VALUE |
                                       StandardAuthorizationActions.DATA_DELETE_VALUE;
                         
    private static final int rInt    = StandardAuthorizationActions.DATA_READ_VALUE;
    private static final int cInt    = StandardAuthorizationActions.DATA_CREATE_VALUE;
    private static final int ruInt   = StandardAuthorizationActions.DATA_UPDATE_VALUE |
                                       StandardAuthorizationActions.DATA_READ_VALUE;
    private static final int rdInt   = StandardAuthorizationActions.DATA_DELETE_VALUE |
                                       StandardAuthorizationActions.DATA_READ_VALUE;
    private static final int crInt   = StandardAuthorizationActions.DATA_CREATE_VALUE |
                                       StandardAuthorizationActions.DATA_READ_VALUE;
    private static final int rudInt  = StandardAuthorizationActions.DATA_READ_VALUE   |
                                       StandardAuthorizationActions.DATA_UPDATE_VALUE |
                                       StandardAuthorizationActions.DATA_DELETE_VALUE;

    public static final AuthorizationActions CRUD = StandardAuthorizationActions.getAuthorizationActions(crudInt);
    public static final AuthorizationActions C    = StandardAuthorizationActions.getAuthorizationActions(cInt);
    public static final AuthorizationActions R    = StandardAuthorizationActions.getAuthorizationActions(rInt);
    public static final AuthorizationActions RU   = StandardAuthorizationActions.getAuthorizationActions(ruInt);
    public static final AuthorizationActions RD   = StandardAuthorizationActions.getAuthorizationActions(rdInt);
    public static final AuthorizationActions CR   = StandardAuthorizationActions.getAuthorizationActions(crInt);
    public static final AuthorizationActions RUD  = StandardAuthorizationActions.getAuthorizationActions(rudInt);
        


    /**
     * Constructor for AuthorizationTestUtil.
     */
    private AuthorizationTestUtil() {
    }

    public static AuthorizationRealm getRealm() {
        return RolePermissionFactory.getRealm();
    }

    public static MetaMatrixPrincipalName createUserName( final String name ) {
        return new MetaMatrixPrincipalName(name,PRINCIPAL_TYPE_USER);
    }

    public static MetaMatrixPrincipalName createGroupName( final String name ) {
        return new MetaMatrixPrincipalName(name,PRINCIPAL_TYPE_GROUP);
    }

    public static void addPrincipalsToPolicy( final AuthorizationPolicy policy, final MetaMatrixPrincipalName[] names ) {
        final Set newPrincipalNames = new HashSet();
        for ( int i=0;i!=names.length; ++i ) {
            newPrincipalNames.add(names[i]);
        }
        policy.addAllPrincipals(newPrincipalNames);
    }

    public static void removePrincipalsFromPolicy( final AuthorizationPolicy policy, final MetaMatrixPrincipalName[] names ) {
        for ( int i=0;i!=names.length; ++i ) {
            policy.removePrincipal(names[i]);
        }
    }
    
    public static void addPermissionToPolicy( final AuthorizationPolicy policy,
                                       final String realmName,
                                       final String resourceName,
                                       final AuthorizationActions actions ) {
        final AuthorizationPermission perm = createPermission(realmName,resourceName,actions);
        policy.addPermission(perm);
    }

    public static AuthorizationPermission createPermission( final String realmName,
                                                            final String resourceName,
                                                            final AuthorizationActions actions ) {
        final AuthorizationResource resource = new DataAccessResource(resourceName);
        final AuthorizationRealm realm = new AuthorizationRealm(realmName);
        final String contentModifier = ""; //$NON-NLS-1$
        return METABASE_FACTORY.create(resource, realm, actions, contentModifier);
    }


}
