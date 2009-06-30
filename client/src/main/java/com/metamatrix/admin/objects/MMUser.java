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

package com.metamatrix.admin.objects;

import org.teiid.adminapi.User;


/** 
 * @since 4.3
 */
public class MMUser extends MMPrincipal implements User {

    /** 
     * @param identifierParts
     * @since 4.3
     */
    public MMUser(String[] identifierParts) {
        super(identifierParts, TYPE_USER);
    }
    
    /**
     * Determine if the given property name is legal for a User. 
     * @param propName The name of a user property.
     * @return <code>true</code> iff the given name is one of
     * the allowed {@link User} properties.
     * @since 4.3
     */
    public static final boolean isUserProperty(String propName) {
        return (propName.equals(COMMON_NAME) ||
                        propName.equals(GIVEN_NAME) ||
                        propName.equals(SURNAME) ||
                        propName.equals(LOCATION) ||
                        propName.equals(TELEPHONE_NUMBER));
    }
}
