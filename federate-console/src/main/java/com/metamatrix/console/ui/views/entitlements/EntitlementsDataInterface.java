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

package com.metamatrix.console.ui.views.entitlements;

import java.util.Collection;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;

import com.metamatrix.console.util.ExternalException;

import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.security.api.AuthorizationPolicyID;

public interface EntitlementsDataInterface {
	
    EntitlementsTableRowData[] getEntitlements() throws AuthorizationException, ExternalException;
    
    int[] getVersionsForVDB(String vdbName) throws AuthorizationException, ExternalException,
                                                    ComponentNotFoundException;
    
    java.util.List /*<AuthorizationPolicyID>*/ getPolicyIDs() throws AuthorizationException, ExternalException;
    
    EntitlementInfo getEntitlementInfo(String entName, String vdbName, int vdbVersion) throws AuthorizationException, 
                                                                                               ExternalException,
                                                                                               ComponentNotFoundException;
    
//    Collection /*<String>*/ getAllEnterpriseUserNames() throws AuthorizationException, ExternalException,
//                                                                ComponentNotFoundException;
    
//    Collection /*<String>*/ getAllEnterpriseGroupNames() throws AuthorizationException, ExternalException,
//                                                                 ComponentNotFoundException;
    
    Collection /*<VirtualDatabase>*/ getAllVDBs() throws AuthorizationException, ExternalException,
                                                          ComponentNotFoundException;
    
    void deleteEntitlement(AuthorizationPolicyID id) throws AuthorizationException, ExternalException,
                                                              ComponentNotFoundException;
    
    boolean doesEntitlementExist(String entName, String vdbName, int vdbVErsion) throws AuthorizationException, ExternalException,
                                                                                          ComponentNotFoundException;
    
    PermissionDataNodeTreeView getTreeViewForData(String vdbName, int vdbVersion, AuthorizationPolicyID policyID) throws AuthorizationException, ExternalException;
}
