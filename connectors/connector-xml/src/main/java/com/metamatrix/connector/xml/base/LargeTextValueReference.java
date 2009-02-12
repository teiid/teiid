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

package com.metamatrix.connector.xml.base;

import com.metamatrix.connector.exception.ConnectorException;

public interface LargeTextValueReference {

    /**
     * Obtain the actual original value that is held by the reference.
     * @return The original large object from the source
     */
    Object getValue();

    /**
     * Obtain the content held in the reference as a string.
     * @return String representation of reference content.
     */
    String getContentAsString() throws ConnectorException;

    /**
     * Get total size of the object.  This is expected to potentially take a long
     * time if the entire object must be scanned to determine the size.
     * @return Size of the object in either bytes or characters, as appropriate
     */
    long getSize();


    /**
     * Determine whether this is a blob or clob type object.  
     * @return True if binary, false if character data
     */
    boolean isBinary();
}


