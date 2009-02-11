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

package com.metamatrix.common.log;

import java.util.Set;
import java.util.Collection;

public interface LogConfiguration extends Comparable, Cloneable {

    boolean isContextDiscarded( String context );

    boolean isLevelDiscarded( int level );

    /**
     * Obtain the set of contexts for messages that are to be discarded.
     * If this method returns an empty set, then messages in all contexts
     * are being recorded; if not empty, then messages with a context in the
     * returned set are discarded and messages for all other contexts recorded.
     * @return the set of contexts for messages that are to be discarded
     */
    Set getDiscardedContexts();

    /**
     * Specify that messages with the input context should be discarded
     * and not recorded.
     * @param context the context to add to the set; this method does nothing
     * if the reference is null
     */
    void discardContext( String context );

    /**
	 * Get the level of detail of messages that are currently being recorded.
	 * @return the level of detail
	 */
    int getMessageLevel();

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
    int compareTo(Object obj);

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    boolean equals(Object obj);

	/**
	 * String representation of logging configuration.
	 * @return String representation
	 */
	String toString();

    Object clone();

    /**
     * Direct the log configuration to record all known logging contexts.
     */
    void recordAllContexts();

    /**
     * Direct the log configuration to discard the given contexts and
     * not record them.
     * @param contexts the collection of contexts that should be discarded.
     */
    void discardContexts(Collection contexts);

    /**
     * Direct the log configuration to record only these contexts.
     * @param contexts the contexts that should be recorded.
     */
    void recordContexts(Collection contexts);

    /**
     * Direct the log configuration to record messages of the given level
     * or above.
     * @param level the lowest level to record.
     */
    void setMessageLevel(int level);
}
