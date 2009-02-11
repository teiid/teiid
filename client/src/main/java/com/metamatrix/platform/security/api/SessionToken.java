/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

/**
 * This class is an immutable identifier for a unique session that also
 * maintains the name of the principal for that session.  It is used internally
 * to MetaMatrix to allow association of a principal name to various activities.
 * 
 * Server-side object should not be returned to the client
 */
public class SessionToken implements Serializable, Comparable<SessionToken>, Cloneable {
    public final static long serialVersionUID = -2853708320435636107L;

    /** The session ID */
    private MetaMatrixSessionID sessionID;
    private String userName;
    
    /**
     * Fake SessionToken representing a trusted user
     */
    public SessionToken() {
    	this.sessionID = new MetaMatrixSessionID(-1);
    	this.userName = "trusted"; //$NON-NLS-1$
    }

    /**
    * The primary constructor that specifies the id, userName, and product info
    * for the session represented by this token.
    * @param id (long) the unique identifier for the session
     * @param userName (String) the userName for this session
     * @throws IllegalArgumentException
     */
     public SessionToken(MetaMatrixSessionID id, String userName){
         this.sessionID = id;
         this.userName = userName;
     }    

    /**
     * Returns unique session identifier
     * @return the session ID
     */
    public MetaMatrixSessionID getSessionID() {
        return this.sessionID;
    }

    /**
     * Returns unique session identifier
     * @return the session ID value
     */
    public String getSessionIDValue() {
        return this.sessionID.toString();
    }

    /**
     * Get the principal name for this session's user.
     * @return the user name
     */
    public String getUsername() {
        return this.userName;
    }
    
	/**
     * Compares this SessionToken to another Object. If the Object is a SessionToken,
     * this function compares the ID and the user account ID.  Otherwise, it throws a
     * ClassCastException (as SessionToken instances are comparable only to
     * other SessionToken instances).  Note:  this method is consistent with
     * <code>equals()</code>.
	 * <p>
     * @param o the object that this instance is to be compared to.
	 * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
	 * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this UserID.
	 */
	public int compareTo(SessionToken that) {
        if ( that == this ) {
            return 0;
        }

        // Check if everything else is equal ...
        int result = this.sessionID.compareTo(that.sessionID);
        return result;
    }

	/**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
	 * <p>
     * @param obj the object that this instance is to be compared to.
	 * @return whether the object is equal to this object.
	 */
	public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if ( obj instanceof SessionToken ) {
            SessionToken that = (SessionToken)obj;
        	return ( this.sessionID.equals(that.sessionID)  );
        }

        // Otherwise not comparable ...
        return false;
    }

    /**
     * Overrides Object hashCode method.
     * @return  a hash code value for this object.
     * @see     Object#hashCode()
     * @see     Object#equals(Object)
	 */
	public int hashCode() {
        return this.sessionID.hashCode();
    }

    /**
     * Returns a string representing the current state of the object.
     */
    public String toString() {
        return "SessionToken[" + getUsername() + "," + getSessionIDValue() + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * Return a cloned instance of this object.
     * @return the object that is the clone of this instance.
     */
    public Object clone() {
        try {
            // Everything is immutable, so bit-wise copy (of references) is okay!
            return super.clone();
        } catch ( CloneNotSupportedException e ) {
        }
        return null;
    }

}
