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

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

/**
 * This class wraps two Collections of <code>String</code> names -
 * the first is a <code>Collection</code> of names of MetaMatrix
 * user principals, the other is a <code>Collection</code> of group
 * principal names. These principal names can then be used, by a client,
 * to retrieve a {@link MetaMatrixPrincipal} object from the server.
 */
public class MetaMatrixPrincipalNames implements Serializable {

    private Set groupNames;
    private Set userNames;

    /**
     * The constructor takes two Sets of names - one of group names, one of
     * user names.  These two sets should never be null (they can be empty),
     * but there are no checks against null sets currently.
     */
    public MetaMatrixPrincipalNames(Set groupPrincipalNames, Set userPrincipalNames){
        if ( groupPrincipalNames == null ) {
            groupNames = new HashSet();
        }
         if ( userPrincipalNames == null ) {
             userNames = new HashSet();
        }
        this.groupNames = groupPrincipalNames;
        this.userNames = userPrincipalNames;
    }

    /**
     * Returns the names of all group principals contained in this
     * object
     * @return currently returns a <code>Set</code> of <code>String</code>
     * names, representing group principals.  However, note that this
     * implementation may change.
     */
    public Collection getGroupPrincipalNames(){
        return this.groupNames;
    }

    /**
     * Returns the names of all user principals contained in this
     * object
     * @return currently returns a <code>Set</code> of <code>String</code>
     * names, representing user principals.  However, note that this
     * implementation may change.
     */
    public Collection getUserPrincipalNames(){
        return this.userNames;
    }

    /**
     * Displays the group and user names
     */
    public String toString(){
        StringBuffer buffer = new StringBuffer();
        buffer.append("MetaMatrixPrincipalNames: {group names: "); //$NON-NLS-1$
        buffer.append(groupNames);
        buffer.append(" }, {user names: "); //$NON-NLS-1$
        buffer.append(userNames);
        buffer.append(" }"); //$NON-NLS-1$
        return buffer.toString();
    }
}



