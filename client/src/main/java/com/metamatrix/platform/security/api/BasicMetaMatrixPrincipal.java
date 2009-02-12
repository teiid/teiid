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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;

public class BasicMetaMatrixPrincipal implements MetaMatrixPrincipal, Serializable {

    private int type;
    private String name;
    private Set unmodifiableGroupNames;

    /**
     * Create a <code>BasicMetaMatrixPrincipal</code> with all attributes required for
     * display in the MetaMatrix Console. 
     * 
     * @param name the name of the principal.
     * @param type the principal type (user or group) see {@link MetaMatrixPrincipal}.
     * @param groupNames the memberships to which this principal belongs (explicitly).
     * @param properties the properties that will be displayed in the Console (location, phone #, etc)
     * for this principal.  <b>NOTE:</b> these properties may be <code>null</code>.
     */
    public BasicMetaMatrixPrincipal( String name, int type, Set groupNames) {
        if ( name == null || name.trim().length() == 0 ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0013));
        }
        if ( name.trim().length() > NAME_LEN_LIMIT ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0014,
                                                NAME_LEN_LIMIT));
        }
        if ( type < TYPE_USER || type > TYPE_ADMIN ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0015));
        }
        this.name = name;
        this.type = type;
        this.unmodifiableGroupNames = Collections.unmodifiableSet(groupNames);
    }

    /**
     * Create a minimal <code>BasicMetaMatrixPrincipal</code>.
     * 
     * <p><b>NOTE:</b> For this object to be displayed properly in the MetaMatrix Console,
     * it's group memberships must be added after creation. This is currently not exposed.</p>
     * 
     * @param name the name of the principal.
     * @param type the principal type (user or group) see {@link MetaMatrixPrincipal}.
     */
    public BasicMetaMatrixPrincipal( String name, int type ) {
        this(name,type,Collections.EMPTY_SET);
    }

    /**
     * Copy CTOR. 
     * @param obj the object to copy
     */
    protected BasicMetaMatrixPrincipal( BasicMetaMatrixPrincipal obj ) {
        if ( obj == null ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0016));
        }
        this.type = obj.getType();
        this.name = obj.getName();
        this.unmodifiableGroupNames = Collections.unmodifiableSet(obj.getGroupNames());
    }

    /**
     * Get the <code>MetaMatrixPrincipalName</code> for this principal.
     * @see MetaMatrixPrincipaName.
     * @return the <code>MetaMatrixPrincipalName</code> for this principal.
     */
    public MetaMatrixPrincipalName getMetaMatrixPrincipalName() {
        return new MetaMatrixPrincipalName(this.name, this.type);
    }

    /**
     * Returns the Principal for each group that this principal is a member of.
     */
    public Set getGroupNames(){
        return unmodifiableGroupNames;
    }

    public boolean equals(Object par1){
        boolean result = false;
        if( this == par1){
            return true;
        }
        if( par1 instanceof BasicMetaMatrixPrincipal){
            if( this.type == ((BasicMetaMatrixPrincipal)par1).getType()){
                if(this.name.compareTo(((BasicMetaMatrixPrincipal)par1).getName()) == 0){
                    result = true;
                }
            }
        }
        return result;
    }

    /**
     * Get the type of principal
     * @return the type for this principal
     */
    public int getType() {
        return type;
    }

    /**
     * Get the String form for the type of principal
     * @return the type for this principal as a String
     */
    public String getTypeLabel() {
        return ( TYPE_NAMES[this.type] );
    }

    /**
     * Returns the name of this principal.
     * @return the name of this principal (never null)
     */
    public String getName(){
        return name;
    }
    public int hashCode(){
        return name.hashCode();
    }
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append("Name=\""); //$NON-NLS-1$
        sb.append(this.getName());
        sb.append("\", Type="); //$NON-NLS-1$
        sb.append( TYPE_NAMES[this.type] );
        sb.append(", Groups="); //$NON-NLS-1$
        sb.append(getGroupNames());
        return sb.toString();
    }

    /**
     * Return a cloned instance of this object.
     * @return the object that is the clone of this instance.
     */
    public Object clone() {
        return new BasicMetaMatrixPrincipal(this);
    }

    /**
     * Merge all of the attributes of the input principal into the target
     * principal.  This method returns a new instance that is the merged
     * result.
     * @param p1 the first principal that is to be merged
     * @param p2 the second principal that is to be merged
     * @return the new MetaMatrixPrincipal instance that is the result of the merge.
     * @throws InvalidMetaMatrixSessionException if the two input MetaMatrixPrincipal
     * instances do not have the same username.
     * @throws IllegalArgumentException if either of the two input principals
     * are null.
     */
    public static MetaMatrixPrincipal merge( MetaMatrixPrincipal p1, MetaMatrixPrincipal p2 )
    throws InvalidSessionException {
        if ( p1 == null || p2 == null ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0017));
        }
        if ( p1.getType() != p2.getType() ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0018, TYPE_NAMES[p1.getType()], TYPE_NAMES[p2.getType()]));
        }

        if ( ! p1.getName().equals( p2.getName() ) ) {
            throw new InvalidSessionException(ErrorMessageKeys.SEC_MEMBERSHIP_0019, CorePlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0019, p1.getName(),
                            p2.getName() ));
        }

        Set groups = new HashSet( p1.getGroupNames() );
        groups.addAll( p2.getGroupNames() );

        return new BasicMetaMatrixPrincipal( p1.getName(), p1.getType(), groups );
    }

}

