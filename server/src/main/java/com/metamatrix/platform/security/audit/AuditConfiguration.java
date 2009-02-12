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

package com.metamatrix.platform.security.audit;

import java.util.Collection;
import java.util.Set;

public interface AuditConfiguration extends Comparable, Cloneable {

    public boolean isContextDiscarded( String context );

    public boolean isLevelDiscarded( int level );

    /**
     * Obtain the set of contexts for messages that are to be discarded.
     * If this method returns an empty set, then messages in all contexts
     * are being recorded; if not empty, then messages with a context in the
     * returned set are discarded and messages for all other contexts recorded.
     * @return the set of contexts for messages that are to be discarded
     */
    public Set getDiscardedContexts();

    /**
     * Specify that messages with the input contexts should be discarded
     * and not recorded.
     * @param contexts the set of contexts that are to be ignored for logging;
     * this method does nothing if the reference is null or empty
     */
    public void discardContexts( Collection contexts );

    /**
     * Specify that messages with the input context should be discarded
     * and not recorded.
     * @param context the context to add to the set; this method does nothing
     * if the reference is null
     */
    public void discardContext( String context );

    /**
     * Specify that messages in all contexts are to be recorded.
     */
    public void recordAllContexts();

    /**
     * Specify that messages in the input context should be recorded rather than
     * discarded.
     * @param context the context for messages that should be recorded; this
     * method does nothing if the reference is null
     */
    public void recordContext( String context );

     /**
     * Specify that messages in the input contexts should be recorded rather than
     * discarded.
     * @param contexts the set of contexts that are to be recorded;
     * this method does nothing if the reference is null or empty
     */
    public void recordContexts( Collection contexts );

	/**
	 * Get the level of detail of messages that are currently being recorded.
	 * @return the level of detail
	 */
    public int getAuditLevel();

    /**
     * Method to set the level of messages that are recorded for this VM.
     * @param newMessageLevel the new level; must be either
     *    <code>AuditLevel.NONE</code>,
     *    <code>AuditLevel.FULL</code>
     * @throws IllegalArgumentException if the level is out of range.
     */
    public void setAuditLevel( int newAuditLevel );

    /**
     * Compares this object to another. If the specified object is an instance of
     * the MetadataID class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).
     * Note:  this method <i>is</i> consistent with <code>equals()</code>, meaning
     * that <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj);

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj);

	/**
	 * String representation of logging configuration.
	 * @return String representation
	 */
	public String toString();

    public Object clone();
}
