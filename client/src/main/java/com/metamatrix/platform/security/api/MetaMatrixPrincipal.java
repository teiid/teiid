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

package com.metamatrix.platform.security.api;

import java.security.Principal;
import java.util.Set;

/**
 * This interface represents an abstract notion of users, groups or applications
 * within the MetaMatrix Security Framework.  MetaMatrixPrincipal is anlogous
 * to java.security.Principal, which it extends from.
 */
public interface MetaMatrixPrincipal extends Principal, Cloneable {

    // JPC 07/07/2005
    // These constants were copied to com.metamtrix.admin.server package
    // (instead of moved - for dependancy reasons) so any changes
    // made here should also be made there.
    // Better yet, these should be moved there and dependanies resolved
    static final int TYPE_USER        = 0;
    static final int TYPE_GROUP       = 1;
    static final int TYPE_ADMIN       = 2;

    public static final String TYPE_LABEL_USER        = "User"; //$NON-NLS-1$
    public static final String TYPE_LABEL_GROUP       = "Group"; //$NON-NLS-1$
    public static final String TYPE_LABEL_ADMIN       = "Admin"; //$NON-NLS-1$


    // User and Group names can be no longer then this
    public static final int NAME_LEN_LIMIT = 1024;

    static final String[] TYPE_NAMES = new String[] {TYPE_LABEL_USER, TYPE_LABEL_GROUP, TYPE_LABEL_ADMIN};

    /**
     * Get the <code>MetaMatrixPrincipalName</code> for this principal.
     * @see MetaMatrixPrincipaName.
     * @return the <code>MetaMatrixPrincipalName</code> for this principal.
     */
    MetaMatrixPrincipalName getMetaMatrixPrincipalName();

    /**
     * Get the type of principal
     * @return the type for this principal
     */
    int getType();

    /**
     * Get the String form for the type of principal
     * @return the type for this principal as a String
     */
    String getTypeLabel();

    /**
     * Returns the Principal for each group that this principal is a member of.
     */
    Set getGroupNames();

    /**
     * Return a cloned instance of this object.
     * @return the object that is the clone of this instance.
     */
    Object clone();
}



