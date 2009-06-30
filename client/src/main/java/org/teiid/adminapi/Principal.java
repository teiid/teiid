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

package org.teiid.adminapi;


/** 
 * A Principal may participate in entitlements (authorization).  A Principal
 * may also posess administrative roles for the system.
 * 
 * <p>The identifier pattern for all principal types is <code>"name"</code>.
 * A name is concidered to be unique across the system for all
 * principal types.</p>
 * @since 4.3
 */
public interface Principal extends AdminObject {

    /** User internal type.  Note that a User can be of type Admin and vice versa. */
    static final int TYPE_USER = 0;
    /** Group internal type */
    static final int TYPE_GROUP = 1;
    /** Admin internal type.  Note that a User can be of type Admin and vice versa. */
    static final int TYPE_ADMIN = 2;

    /** Display String for User type */
    static final String TYPE_LABEL_USER = "User"; //$NON-NLS-1$
    /** Display String for Group type */
    static final String TYPE_LABEL_GROUP = "Group"; //$NON-NLS-1$
    /** Display String for Admin type */
    static final String TYPE_LABEL_ADMIN = "Admin"; //$NON-NLS-1$

    /** User and Group names can be no longer then this */
    static final int NAME_LEN_LIMIT = 32;

    /** 
     * Get the Principal type for this principal.
     * @return the internal type of this user.
     * @since 4.3
     */
    int getType();

    /** 
     * Get the Principal type String for this principal.
     * @return the String representation type of this user.
     * @since 4.3
     */
    String getTypeLabel();

}
