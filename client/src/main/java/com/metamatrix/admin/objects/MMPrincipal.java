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

import org.teiid.adminapi.Principal;


/** 
 * @since 4.3
 */
public class MMPrincipal extends MMAdminObject implements Principal {

    private static final String[] TYPE_NAMES = new String[] { TYPE_LABEL_USER, TYPE_LABEL_GROUP, TYPE_LABEL_ADMIN };

    private int type;

    /** 
     * Ctor
     * @param principalName The name of the {@link Principal}.
     * @param type The value of the {@link Principal}.
     * @since 4.3
     */
    public MMPrincipal(String[] principalName, int type) {
        super(principalName);
        this.type = type;
    }

    /** 
     * @see com.metamatrix.admin.objects.MMAdminObject#toString()
     * @since 4.3
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[Name=\""); //$NON-NLS-1$
        sb.append(this.getName());
        sb.append("\" - Type=\""); //$NON-NLS-1$
        sb.append( TYPE_NAMES[this.type] );
        sb.append("\"]"); //$NON-NLS-1$
        return sb.toString();
    }

    /** 
     * @see org.teiid.adminapi.Principal#getType()
     * @since 4.3
     */
    public int getType() {
        return type;
    }

    /** 
     * @see org.teiid.adminapi.Principal#getTypeLabel()
     * @since 4.3
     */
    public String getTypeLabel() {
        return TYPE_NAMES[this.type];
    }
    
    /**
     * Determine whether the given <code>className</code>
     * is either of {@link #TYPE_USER} or {@link #TYPE_GROUP}. 
     * @param className the className in question.
     * @return <code>true</code> iff the given type represents
     * one or the other; User or Group.
     * @since 4.3
     */
    public static boolean isUserOrGroup(String className) {
        try {
            int type = MMAdminObject.getObjectType(className);
            return (type == MMAdminObject.OBJECT_TYPE_USER || type == MMAdminObject.OBJECT_TYPE_GROUP);
        } catch (Exception e) {
            return false;
        }
    
    }

}
