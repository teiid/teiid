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

package com.metamatrix.common.comm.platform;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.platform.admin.apiimpl.IAdminHelper;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;


/** 
 * @since 4.3
 */
public class FakeAdminHelper implements
                            IAdminHelper {
    private String userName;
    private Set userRoleNames;

    /** 
     * 
     * @since 4.3
     */
    public FakeAdminHelper(String userName, Set userRoleNames) {
        this.userName = userName;
        this.userRoleNames = userRoleNames;
    }

    /** 
     * @see com.metamatrix.platform.admin.apiimpl.IAdminHelper#checkForRequiredRole(com.metamatrix.platform.security.api.SessionToken, java.lang.String)
     * @since 4.3
     */
    public void checkForRequiredRole(SessionToken adminToken,
                                         String roleName) throws AuthorizationException, ComponentNotFoundException {
        if ( adminToken.getUsername().equals(userName) ) {
            Iterator iter = userRoleNames.iterator();
            while ( iter.hasNext() ) {
                String userRole = (String)iter.next();
                if ( userRole.equals(roleName) ) {
                    return;
                }
            }
            throw new AuthorizationException("User does not have required role [" + roleName + "]."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        throw new AuthorizationException("User names are not equal. Incoming [" + adminToken.getUsername() +  //$NON-NLS-1$
                                         "] Expected [" + userName + "]"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** 
     * @see com.metamatrix.platform.admin.apiimpl.IAdminHelper#checkForRequiredRole(com.metamatrix.platform.security.api.SessionToken, java.util.Set)
     * @since 4.3
     */
    public void checkForRequiredRole(SessionToken adminToken,
                                         Set roleNames) throws AuthorizationException, ComponentNotFoundException {
        Iterator roles = roleNames.iterator();
        while ( roles.hasNext() ) {
            this.checkForRequiredRole(adminToken, (String) roles.next());
        }
    }

    /** 
     * @see com.metamatrix.platform.admin.apiimpl.IAdminHelper#validateSession(com.metamatrix.platform.security.api.MetaMatrixSessionID)
     * @since 4.3
     */
    public SessionToken validateSession(MetaMatrixSessionID sessionID) throws InvalidSessionException, ComponentNotFoundException {
        return new SessionToken(sessionID, "fake_cluster", "fake", new Properties()); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
