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

package com.metamatrix.platform.security.util;

import java.io.Serializable;

import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.AuthorizationPermissionFactory;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.AuthorizationResource;
import com.metamatrix.platform.security.api.DataAccessResource;

/**
 * This class is the factory for RolePermission instances.
 */
public class RolePermissionFactory implements AuthorizationPermissionFactory, Serializable {

    // Administrative Roles know their realm. Roles come with the installation
    // and are not to be created.
    private static final String ROLE_REALM_NAME = "AdminRoleRealm"; //$NON-NLS-1$

    // And niether are their realms
    private static final AuthorizationRealm ROLE_REALM;

    static {
        ROLE_REALM = new AuthorizationRealm(ROLE_REALM_NAME);
        ROLE_REALM.setDescription("The administrative role realm."); //$NON-NLS-1$
    }

    /**
     * Get the class that this factory creates instances of.
     * @return the class of the instances returned by this factory's <code>create</code> methods.
     */
    public Class getPermissionClass() { return RolePermission.class; }

    /**
     * Create the AuthorizationResource type for the permission type that this factory creates instances of.
     * @return A new resource instance of the appropriate type.
     */
    public AuthorizationResource createResource(String name) {
        return new DataAccessResource(name);
    }

    /**
     * Create a new authorization permission for the specified role.
     * @param roleName the new role name
     * @param realm the realm is thrown away. The <code>RolePermissionFactory</code>
     * knows the realm that roles belong.
     */
    public AuthorizationPermission create(String roleName, AuthorizationRealm realm) {
        return new RolePermission(new DataAccessResource(roleName), ROLE_REALM, this.getClass().getName());
    }

    /**
     * Create a new authorization permission for the specified resource.
     * @param roleName the name for the resource.
     * @param realm the realm is thrown away. The <code>RolePermissionFactory</code>
     * knows the realm that roles belong.
     * @param actions the actions for the resource - ignored in this factory.
     * @param contentModifier the content modifier (may be null) - ignored in this factory.
     */
    public AuthorizationPermission create(AuthorizationResource role, AuthorizationRealm realm, AuthorizationActions actions, String contentModifier) {
        return new RolePermission(role, ROLE_REALM,this.getClass().getName());
    }

    /**
     * Create a new authorization permission for the specified resource.
     * @param roleName the new resource name
     * @param realm the realm is thrown away. The <code>RolePermissionFactory</code>
     * knows the realm that roles belong.
     * @param actions the actions for the resource - ignored in this factory.
     */
    public AuthorizationPermission create(String roleName, AuthorizationRealm realm, AuthorizationActions actions) {
        return new RolePermission(new DataAccessResource(roleName),ROLE_REALM,this.getClass().getName());
    }

    /**
     * Get the name of the Realm under which the factory creates its roles.
     * @return the name of this role's realm.
     */
    public static String getRealmName() {
        return ROLE_REALM_NAME;
    }

    /**
     * Get the Realm under which the factory creates its roles.
     * @return this role's realm.
     */
    public static AuthorizationRealm getRealm() {
        return ROLE_REALM;
    }
}


