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

/*
 * Date: Feb 11, 2004
 * Time: 9:33:14 AM
 */
package com.metamatrix.platform.security.api.service;

import java.io.Serializable;

/**
 * Interface AuthenticationToken.
 *
 * <p></p>
 */
public interface AuthenticationToken extends Serializable {

    /**
     * Get the payload token that has been successfully authentcated by a
     * membership domain.
     * @return The successfully authenticated token unmodified.
     */
    Serializable getPayload();
    
    /**
     * Get the <b>exact</b> username of the authenticated user as it
     * is known to the authenticating membership domain.
     * <br>Will be <code>null</code> if the user was <b>not</b>
     * authenticated. 
     * @return The username (including case) of this authenticated
     * user exactly as it is known by the authenticating memebership domain.
     * @since 5.0
     */
    String getUserName();

    /**
     * Find out whether or not the user was authenticated. 
     * @return <code>true</code> iff the membership domain was able 
     * to authenticate this user. 
     * @since 5.0
     */
    boolean isAuthenticated();
}
