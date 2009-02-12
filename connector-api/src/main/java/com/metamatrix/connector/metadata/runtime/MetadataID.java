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

package com.metamatrix.connector.metadata.runtime;

import java.util.List;

import com.metamatrix.connector.exception.ConnectorException;

/**
 * Represents a runtime metadata identifier.
 */
public interface MetadataID {

    public static final int TYPE_ELEMENT = 0;
    public static final int TYPE_GROUP = 1;
    public static final int TYPE_PROCEDURE = 2;
    public static final int TYPE_PARAMETER = 3;

    /**
     * Get the type of metadataID
     * @return ID type
     * @see #TYPE_ELEMENT
     * @see #TYPE_GROUP
     * @see #TYPE_PROCEDURE
     * @see #TYPE_PARAMETER
     */
    int getType();

    /**
     * Get a list of child IDs from this ID.  A group metadata ID will
     * return child element IDs.  An element ID will return no child IDs.
     * A procedure ID will return the IDs of it's parameters.
     * @return List of MetadataID, which may be empty but never null
     */
    List getChildIDs() throws ConnectorException;

    /**
     * Get the parent ID if one exists
     * @return Parent ID or null if none exists
     */
    MetadataID getParentID() throws ConnectorException;
    
    /**
     * Get the short name from the metadataID
     * @return String shortName
     */
    String getName();
    
    /**
     * Get the full name from the metadataID
     * @return String fullName
     */
    String getFullName();
}
