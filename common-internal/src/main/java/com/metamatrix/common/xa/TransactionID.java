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

/*
 * Date: Jan 2, 2003
 * Time: 3:27:45 PM
 */
package com.metamatrix.common.xa;

import java.io.Serializable;

public class TransactionID implements Serializable {

    private String ID;

    /**
     * Create a new instance of TranscationID
     *
     * @param id uniqueID
     */
    public TransactionID(String id) {
        this.ID = id;
    }

    /**
     * Returns the ID value.
     * @return String ID value
     */
    public String getID() {
        return ID;
    }

    /**
     * Get hash code for object
     * @return Hash code
     */
    public int hashCode() {
        return this.ID.hashCode();
    }

    /**
     * Returns <code>true</code> if this object is equal to the other object.
     * Equality is based on the ID value.
     *
     * @param obj Object to compare with
     * @return boolean indicating if objects are equal
     */
    public boolean equals(Object obj) {
        if ( this == obj ) {
            return true;
        }
        if (!(obj instanceof TransactionID)) {
            return false;
        }

        return ((TransactionID) obj).ID.equals(ID);
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of this instance.
     */
    public String toString() {
        return this.ID;
    }

}
