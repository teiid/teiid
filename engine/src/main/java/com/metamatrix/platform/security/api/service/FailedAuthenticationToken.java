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
 * Date: Dec 1, 2003
 * Time: 1:26:57 PM
 */
package com.metamatrix.platform.security.api.service;

import java.io.Serializable;

/**
 * FailedAuthenticationToken
 *
 * <p>Marker indicating failed authentication attempt.  Membership SPI
 * domain implementations should wrap the <code>Serializable</code> payload
 * in an instance of this class to signify to the Membgership service that
 * the domian failed to authenticate the given payload token.</p>
 *
 * <p>This wrapper class will not be exposed outside of the Mebership framework.</p>
 */
public final class FailedAuthenticationToken implements AuthenticationToken {

    /**
     * FailedAuthenticationToken
     *
     * @param payload The failed authentication token - may be null.
     */
    public FailedAuthenticationToken() {
    }

    /**
     * Get the payload token that failed to authentcation by a
     * membership domain.
     *
     * @return The failed authentication token unmodified - may be null.
     */
    public Serializable getPayload() {
        return null;
    }

    /**
     * Get the <b>exact</b> username of the authenticated user as it
     * is known to the authenticating membership domain.
     * <p><b>Will be <code>null</code> if the user was <b>not</b>
     * authenticated. </b></p>
     * @return The username (including case) of this authenticated
     * user exactly as it is known by the authenticating memebership domain.
     * @since 5.0
     */
    public String getUserName() {
        return null;
    }

    /**
     * The attempt to authenticate the given payload failed.
     * @return <code>false</code> - always.
     */
    public boolean isAuthenticated() {
        return false;
    }
}
