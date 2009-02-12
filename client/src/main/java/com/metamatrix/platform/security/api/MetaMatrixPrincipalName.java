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

public class MetaMatrixPrincipalName implements Serializable {

    private int type;
    private String name;

    public MetaMatrixPrincipalName( String name, int type ) {
        if ( name == null || name.trim().length() == 0 ) {
            throw new IllegalArgumentException("illegal name " + name); //$NON-NLS-1$
        }
        if ( name.trim().length() > MetaMatrixPrincipal.NAME_LEN_LIMIT ) {
            throw new IllegalArgumentException("name too long " + name); //$NON-NLS-1$
        }
        if ( type < MetaMatrixPrincipal.TYPE_USER || type > MetaMatrixPrincipal.TYPE_ADMIN ) {
            throw new IllegalArgumentException("invalid type " + type); //$NON-NLS-1$
        }
        this.name = name;
        this.type = type;
    }

    protected MetaMatrixPrincipalName( MetaMatrixPrincipalName obj ) {
        if ( obj == null ) {
            throw new IllegalArgumentException("argument cannot be null"); //$NON-NLS-1$
        }
        this.type = obj.getType();
        this.name = obj.getName();
    }

    public boolean equals(Object obj){
        if( this == obj ){
            return true;
        }

        if(!(obj instanceof MetaMatrixPrincipalName)){
        	return false;
        }
        MetaMatrixPrincipalName that = (MetaMatrixPrincipalName) obj;
        return this.type == that.type  && this.name.compareTo(that.name) == 0;
    }

    /**
     * Compares this object to another. If the specified object is
     * an instance of the MetaMatrixPrincipalName class, then this
     * method compares the contents; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).
     * <p>
     * Note:  this method <i>is</i> consistent with
     * <code>equals()</code>, meaning
     *  that
     * <code>(compareTo(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        MetaMatrixPrincipalName that = (MetaMatrixPrincipalName)obj; // May throw ClassCastException
        int comp = this.name.compareTo(that.name);
        if ( comp == 0 ) {
            comp = this.type - that.type;
        }
        return comp;
    }

    /**
     * Get the type of principal
     * @return the type for this principal
     */
    public int getType() {
        return type;
    }

    /**
     * Get the String form for the type of principal.
     * @return the type for this principal as a String
     */
    public String getTypeLabel() {
        return MetaMatrixPrincipal.TYPE_NAMES[this.type];
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
        sb.append("[Name=\""); //$NON-NLS-1$
        sb.append(this.getName());
        sb.append("\" - Type=\""); //$NON-NLS-1$
        sb.append( MetaMatrixPrincipal.TYPE_NAMES[this.type] );
        sb.append("\"]"); //$NON-NLS-1$
        return sb.toString();
    }

}



