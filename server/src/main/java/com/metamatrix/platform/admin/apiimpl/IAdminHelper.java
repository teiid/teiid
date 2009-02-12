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

package com.metamatrix.platform.admin.apiimpl;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;

/** 
 * @since 4.3
 */
public interface IAdminHelper {
    
    /**
     * Checks that user is in the necessary role, throws an exception if
     * not.  Methods of this administrative API may require that a caller be
     * in one or more security roles.
     * @param adminToken a valid SessionToken object representing the session
     * of the caller attempting an administrative operation
     * @param roleName String name of role to be checked for caller membership
     * @throws AuthorizationException if caller is <i>not</i> in the role, and
     * therefore not authorized to make the operation
     * @throws ComponentNotFoundException if the authorization service could
     * not be communicated with due to a bad service instance or proxy
     */
    void checkForRequiredRole(SessionToken adminToken, String roleName)
    throws AuthorizationException, ComponentNotFoundException;
        
    /**
     * Get the <code>SessionToken</code> and validate that the session is active
     * for the specified <code>MetaMatrixSessionID</code>.
     * @param sessionID the <code>MetaMatrixSessionID</code> for the session in
     * question.
     * @return The <code>SessionToken</code> for the session in question.
     * @throws InvalidSessionException If session has expired or doesn't exist
     * @throws ComponentNotFoundException If couldn't find needed service component
     */
    SessionToken validateSession(MetaMatrixSessionID sessionID)
    throws InvalidSessionException, ComponentNotFoundException;
}