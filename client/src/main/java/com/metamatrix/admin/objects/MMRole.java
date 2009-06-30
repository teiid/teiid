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

import java.util.Properties;

import org.teiid.adminapi.Role;



/** 
 * @since 4.3
 */
public class MMRole extends MMAdminObject implements Role {
    /** 
     * Ctor
     * @param identifier the role identifier should contain the whole
     * dotted notation in one field of the identifier so that the name
     * contains all components.
     * @since 4.3
     */
    public MMRole(String[] identifier) {
        super(identifier);
    }
    
    /**
     * Get the names of all administrative roles in the system. 
     * @return the array of all role names suitable for adding to Principals.
     * @since 4.3
     */
    public static String[] getAvailableRoles() {
        return new String[] {Role.ADMIN_PRODUCT, Role.ADMIN_SYSTEM, Role.ADMIN_READONLY};
    }

    /** 
     * @see org.teiid.adminapi.AdminObject#getIdentifier()
     * @since 4.3
     */
    public String getIdentifier() {
        return super.getIdentifier();
    }

    /** 
     * @see org.teiid.adminapi.AdminObject#getName()
     * @since 4.3
     */
    public String getName() {
        // A Role name should not be broken into components
        // Role name should have have complete, dotted notation.
        return super.getIdentifier();
    }

    /** 
     * @see org.teiid.adminapi.AdminObject#getProperties()
     * @since 4.3
     */
    public Properties getProperties() {
        return null;
    }

    /** 
     * @see org.teiid.adminapi.AdminObject#getPropertyValue(java.lang.String)
     * @since 4.3
     */
    public String getPropertyValue(String name) {
        return null;
    }

    /** 
     * @see com.metamatrix.admin.objects.MMAdminObject#toString()
     * @since 4.3
     */
    public String toString() {
        return super.getIdentifier();
    }

}
