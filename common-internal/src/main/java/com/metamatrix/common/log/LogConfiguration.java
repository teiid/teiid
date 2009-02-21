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

package com.metamatrix.common.log;

import java.util.Set;
import java.util.Collection;

public interface LogConfiguration extends Comparable, Cloneable {

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
     * Direct the log configuration to record all known logging contexts.
     */
    void recordAllContexts();

    /**
     * Clone the object.
     * @return
     */
    Object clone();
    
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
    
    
    boolean isMessageToBeRecorded(String context, int msgLevel);
}
