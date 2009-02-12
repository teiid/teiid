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

package com.metamatrix.console.ui.views.entitlements;

import com.metamatrix.platform.admin.api.PermissionTreeView;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;

public class EntitlementInfo {
    private PermissionTreeView treeView;
    private AuthorizationPolicyID policyID;
    private AuthorizationPolicy policy;
    private String entitlementName;
    private String entitlementDescription;
    private String vdbName;
    private int vdbVersion;

    //NOTE-- these lists are expected to each be kept in alphabetical order
    private java.util.List /*<String>*/ enterpriseGroups;

    public EntitlementInfo(PermissionTreeView treeVw, AuthorizationPolicyID id,
            AuthorizationPolicy pol,
            String entName, String entDesc, String vName, int vVers,
            java.util.List /*<String>*/ rGroups) {
        super();
        treeView = treeVw;
        policyID = id;
        policy = pol;
        entitlementName = entName;
        entitlementDescription = entDesc;
        vdbName = vName;
        vdbVersion = vVers;
        enterpriseGroups = rGroups;
    }

    public PermissionTreeView getTreeView() {
        return treeView;
    }

    public AuthorizationPolicyID getPolicyID() {
        return policyID;
    }

    public AuthorizationPolicy getPolicy() {
        return policy;
    }

    public String getEntitlementName() {
        return entitlementName;
    }

    public String getEntitlementDescription() {
        return entitlementDescription;
    }

    public String getVDBName() {
        return vdbName;
    }

    public int getVDBVersion() {
        return vdbVersion;
    }

    public java.util.List getEnterpriseGroups() {
        return enterpriseGroups;
    }
}
