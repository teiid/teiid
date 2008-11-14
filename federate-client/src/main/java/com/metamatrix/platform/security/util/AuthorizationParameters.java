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

package com.metamatrix.platform.security.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * AuthorizationParameters
 *
 * Contains collections of changes to be made for an administrative action on an
 * {@link com.metamatrix.platform.security.api.AuthorizationPolicy}.
 */
public class AuthorizationParameters implements Serializable {

    private final String policyName;
    private final Collection addedUsers;
    private final Collection removedUsers;
    private final Collection addedPermissions;
    private final Collection removedPermissions;
    private final SessionToken token;

    /**
     * Construct an instance of AuthorizationParameters for adding or changing an <code>AuthorizationPolicy</code>.
     *
     * @param policyName         The authorization policy that will be affected.
     * @param addedUsers         The users that should be added to the policy - may be empty but not null.
     * @param removedUsers       The users that should be removed from the policy - may be empty but not null.
     * @param addedPermissions   The
     *                           {@link com.metamatrix.platform.security.api.AuthorizationPermission AuthorizationPermissions}
     *                           that should be added to the policy - may be empty but not null.
     * @param removedPermissions The
     *                           {@link com.metamatrix.platform.security.api.AuthorizationPermission AuthorizationPermissions}
     *                           that should be removed from the policy - may be empty but not null.
     * @param token              The {@link com.metamatrix.platform.security.api.SessionToken} of the
     *                           administrator initiating the call.
     */
    public AuthorizationParameters(final String policyName,
                                   final Collection addedUsers,
                                   final Collection removedUsers,
                                   final Collection addedPermissions,
                                   final Collection removedPermissions,
                                   final SessionToken token) {
        ArgCheck.isNotNull(policyName);
        ArgCheck.isNotNull(token);

        this.policyName = policyName;
        this.addedUsers = addedUsers;
        this.removedUsers = removedUsers;
        this.addedPermissions = addedPermissions;
        this.removedPermissions = removedPermissions;
        this.token = token;
    }

    /**
     * Construct an instance of AuthorizationParameters for deleting an <code>AuthorizationPolicy</code>.
     *
     * @param policyName The authorization policy that will be affected.
     * @param token      The {@link com.metamatrix.platform.security.api.SessionToken} of the
     *                   administrator initiating the call.
     */
    public AuthorizationParameters(final String policyName, final SessionToken token) {
        this(policyName,
             Collections.EMPTY_SET,
             Collections.EMPTY_SET,
             Collections.EMPTY_SET,
             Collections.EMPTY_SET,
             token);
    }

    /**
     * Get the collection of added users, if any.
     *
     * @return A <code>Collection</code> of <code>String</code> names of added users or an empty collection
     *         if no users are to be added in this operation.
     */
    public Collection getAddedUsers() {
        return addedUsers;
    }

    /**
     * Get the collection of removed users, if any.
     *
     * @return A <code>Collection</code> of <code>String</code> names of removed users or an empty collection
     *         if no users are to be removed in this operation.
     */
    public Collection getRemovedUsers() {
        return removedUsers;
    }

    /**
     * Get the collection of {@link com.metamatrix.platform.security.api.AuthorizationPermission AuthorizationPermissions}
     * to be added, if any.
     *
     * @return A <code>Collection</code> of {@link com.metamatrix.platform.security.api.AuthorizationPermission AuthorizationPermissions}
     *         of added <code>AuthorizationPermission</code>s or an empty collection if no
     *         <code>AuthorizationPermission</code>s are to be added in this operation.
     */
    public Collection getAddedPermissions() {
        return addedPermissions;
    }

    /**
     * Get the collection of {@link com.metamatrix.platform.security.api.AuthorizationPermission AuthorizationPermissions}
     * to be removed, if any.
     *
     * @return A <code>Collection</code> of {@link com.metamatrix.platform.security.api.AuthorizationPermission AuthorizationPermissions}
     *         of removed <code>AuthorizationPermission</code>s or an empty collection if no
     *         <code>AuthorizationPermission</code>s are to be removed in this operation.
     */
    public Collection getRemovedPermissions() {
        return removedPermissions;
    }

    /**
     * Get the name of the <code>AuthorizationPolicy</code> that will be affected by this action.
     *
     * @return The <code>AuthorizationPolicy</code> name.
     */
    public String getPolicyName() {
        return policyName;
    }

    /**
     * Get the user name of the administrator initiating this action.
     *
     * @return The user name of the administrator.
     */
    public String getGrantor() {
        return token.getUsername();
    }

    /**
     * Get the <code>SessionToken</code> of the administrator initiating this
     * action.
     *
     * @return The <code>SessionToken</code> of the administrator.
     */
    public SessionToken getToken() {
        return token;
    }

}
