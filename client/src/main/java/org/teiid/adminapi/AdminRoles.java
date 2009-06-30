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

import java.util.HashSet;
import java.util.Set;

/**
 * Static class that lists the roles allowable in the MetaMatrix system.
 * 
 * <p>This class can be used to get a list of all allowable administrative role
 * names or the name of one role to assign to a principal.</p>
 * @since 4.3
 */
public class AdminRoles {
    private static final Set roleSet;

    static {
        roleSet = new HashSet();
        roleSet.add(RoleName.ADMIN_SYSTEM);
        roleSet.add(RoleName.ADMIN_PRODUCT);
        roleSet.add(RoleName.ADMIN_READONLY);
    }

    /**
     * Get the set of static MetaMatrix administrative roles known to the system.
     * @return the <code>Set</code> of <code>String</code> role names.
     * @since 4.3
     */
    public static Set getAllRoleNames() {
        return roleSet;
    }

    /**
     * Determine whether an admin role exists by the given <code>roleName</code>.
     * @param roleName the name for which to validate.
     * @return <code>true</code> iff an admin role exists with the given role name.
     * @since 4.3
     */
    public static boolean containsRole(String roleName) {
        return roleSet.contains(roleName);
    }

    /**
     * Static class that defines defines the allowed administrative roles
     * for the MetaMatrix system.
     * @since 4.3
     */
    public static class RoleName {
        /** System admin role name */
        public static final String ADMIN_SYSTEM                 = "Admin.SystemAdmin"; //$NON-NLS-1$
        /** Product admin role name */
        public static final String ADMIN_PRODUCT                = "Admin.ProductAdmin"; //$NON-NLS-1$
        /** Read-only admin role name */
        public static final String ADMIN_READONLY               = "Admin.ReadOnlyAdmin"; //$NON-NLS-1$
        
        public static final String ANONYMOUS 					= "Anonymous"; //$NON-NLS-1$ 
    }
}
