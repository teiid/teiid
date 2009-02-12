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

package com.metamatrix.platform.admin.api;

import java.util.Collection;
import java.util.Set;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;
import com.metamatrix.platform.security.api.AuthorizationRealm;


/** 
 * 
 */
public interface AuthorizationEditor {
    AuthorizationPolicy createAuthorizationPolicy(AuthorizationPolicyID policyID);
    AuthorizationPolicy clonePolicyPermissions(AuthorizationPolicy sourcePolicy,
                                               AuthorizationPolicy targetPolicy,
                                               AuthorizationRealm targetRealm,
                                               Set allPaths,
                                               EntitlementMigrationReport rpt);
    AuthorizationPolicy clonePolicyPrincipals(AuthorizationPolicy sourcePolicy,
                                                     AuthorizationPolicy targetPolicy);
    AuthorizationPolicy setDescription( AuthorizationPolicy policy, String description );
    ModificationActionQueue getDestination();
    AuthorizationPolicy removePrincipals( AuthorizationPolicy policy, Set principals);
    AuthorizationPolicy addAllPrincipals( AuthorizationPolicy policy, Set principals );
    Collection modifyPermissions(PermissionTreeView treeView, AuthorizationPolicy policy);
    void remove(AuthorizationPolicyID id);
}
