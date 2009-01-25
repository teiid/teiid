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

package com.metamatrix.platform.admin.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.common.util.MultipleRequestConfirmation;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;

public interface MembershipAdminAPI extends SubSystemAdminAPI {

	List getDomainNames( )
	throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, MetaMatrixSecurityException;

	Set getGroupsForDomain(String domainName)
	throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, MetaMatrixSecurityException;

	MetaMatrixPrincipal getUserPrincipal(String principalName)
	throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, MetaMatrixSecurityException;

    MultipleRequestConfirmation getUserPrincipals(Collection userNames)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, MetaMatrixSecurityException;
    
    Collection getGroupPrincipalNames()
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, MetaMatrixSecurityException;

    MultipleRequestConfirmation getGroupPrincipals(Collection groupNames)
    throws AuthorizationException, InvalidSessionException, MetaMatrixComponentException, MetaMatrixSecurityException;
    
    Serializable authenticateUser(String username, Credentials credential, Serializable trustePayload, String applicationName) 
    throws MetaMatrixComponentException, MembershipServiceException;

}

