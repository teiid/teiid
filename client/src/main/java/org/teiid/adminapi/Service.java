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

package org.teiid.adminapi;

import java.util.Date;

/**
 * Represents a service in the MetaMatrix system.
 * 
 * <p>The unique identifier pattern is [host]<{@link #DELIMITER}>[process]<{@link #DELIMITER}>[Service Name]
 * when running against a MetaMatrix server. The [Service Name] can itself have spaces in the name.
 * In the case of the MM Query, a Service does not apply as MM Query is not running within a MM Server VM.</p>
 * 
 * @since 4.3
 */
public interface Service extends
                                 AdminObject {
    /**Registered by not initialized*/
    public static final int STATE_NOT_INITIALIZED = 0;
    /**Open and running*/
    public static final int STATE_OPEN = 1;
    /**Registered but closed*/
    public static final int STATE_CLOSED = 2;
    /**Failed after running successfully*/
    public static final int STATE_FAILED = 3;
    /**Failed during initialization*/
    public static final int STATE_INIT_FAILED = 4;
    /**Not registered*/
    public static final int STATE_NOT_REGISTERED = 5;
    /**Running, but the underlying data source is unavailable*/
    public static final int STATE_DATA_SOURCE_UNAVAILABLE = 6;
    /**Running, not deployed*/
    public static final int STATE_NOT_DEPLOYED = 7;
    
    /**
     * Retrieve the current connector state.  This will be one of the constants: 
     * {@link DQP#STATE_OPEN DQP.STATE_OPEN}.
     * {@link DQP#STATE_NOT_INITIALIZED DQP.STATE_NOT_INITIALIZED}.
     * {@link DQP#STATE_CLOSED DQP.STATE_CLOSED}.
     * {@link DQP#STATE_FAILED DQP.STATE_FAILED}.
     * {@link DQP#STATE_INIT_FAILED DQP.STATE_INIT_FAILED}.
     * {@link DQP#STATE_NOT_REGISTERED DQP.STATE_NOT_REGISTERED}.
     * {@link DQP#STATE_DATA_SOURCE_UNAVAILABLE DQP.STATE_DATA_SOURCE_UNAVAILABLE}.
     * {@link DQP#STATE_NOT_DEPLOYED DQP.STATE_NOT_DEPLOYED}.
     * @return current connector state.
     */
    int getState();

    /**
     * Retrieve the current connector state as a printable <code>String</code>.
     * @return current connector state in String form.
     */
    String getStateAsString();

    /**
     * Returns time of last state change.
     * 
     * @return time of last state change.
     * @since 4.3
     */
    Date getStateChangedTime();

    /**
     * Returns the description
     * 
     * @return description
     */
    String getDescription();
    
    /**
     * Get the component type identifier for this service {@link ComponentType}. 
     * @return the Component Type identifier which can be used to
     * find the ComponentType.
     * @since 6.1
     */
    String getComponentTypeName();

}
