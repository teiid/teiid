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
package com.metamatrix.platform.security.membership.service;

import java.io.Serializable;

import com.metamatrix.platform.security.api.SecurityMessagesKeys;
import com.metamatrix.platform.security.api.SecurityPlugin;

/**
 * SuccessfulAuthenticationToken.
 *
 * <p>Marker indicating successful authentication attempt.  Membership SPI
 * domain implementations should wrap the <code>Serializable</code> payload
 * in an instance of this class to signify to the Membgership service that
 * the domian successfully authenticated the given payload token.</p>
 * 
 * <p>As well as providing a holder for the <code>Serializable</code> payload,
 * which may have been augmented or replaced by the authenticating membership
 * domain, this class provides a holder for the user name of the authenticated
 * user that may be used in the MetaMatrix system for such things as session
 * tracking and authorization policies.</p>
 * 
 * <p>This wrapper class will not be exposed outside of the Membership framework.</p>
 */
public final class SuccessfulAuthenticationToken implements AuthenticationToken {
    private Serializable payload;
    private String username;
    private String domainName;

    /**
     * SuccessfulAuthenticationToken
     * 
     * <p>Indicate that successful user authentication has occurred.</p>
     * 
     * <p>MetaMatrix must know the user name of every user connected to the system.
     * In particular, when MetaMatrix authorization policies (entitlements) are
     * created, the users and groups that are assigned to these policies come from
     * the membership domain.</p>
     *
     * @param payload The successfully authenticated token. May be <code>null</code>.
     * @param username The username of the <i>authenticated</i> user
     * exactly as it is known by the authenticating membership domain. May <b>not</b>
     * be <code>null</code>.
     */
    public SuccessfulAuthenticationToken(final Serializable payload, final String username) {
        if (username == null || username.trim().length() == 0) {
            throw new IllegalArgumentException(SecurityPlugin.Util.getString(SecurityMessagesKeys.SEC_API_0061));
        }
        this.payload = payload;
        this.username = username;
    }

    /**
     * Get the payload token that has been successfully authenticated by a
     * membership domain.
     * @return The successfully authenticated token unmodified.
     */
    public Serializable getPayload() {
        return payload;
    }

    /**
     * Get the <b>exact</b> username of the authenticated user as it
     * is known to the authenticating membership domain.
     * <p><b>Will be <code>null</code> if the user was <b>not</b>
     * authenticated. </b></p>
     * @return The username (including case) of this authenticated
     * user exactly as it is known by the authenticating membership domain.
     * @since 5.0
     */
    public String getUserName() {
        return username;
    }

    /**
     * The attempt to authenticate the given payload was successful.
     *
     * @return <code>true</code> - always.
     */
    public boolean isAuthenticated() {
        return true;
    }
    
    public String getDomainName() {
        return domainName;
    }
    
    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }
}
