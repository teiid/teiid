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

package org.teiid.connector.metadata.runtime;

import org.teiid.connector.api.ConnectorException;

/**
 * Helper methods that can be used to access runtime metadata.
 */
public interface RuntimeMetadata {

    /**
     * Look up an object by identifier
     * @param fullName
     * @return The object
     */
    Group getGroup(String fullName) throws ConnectorException;

    /**
     * Look up an object by identifier
     * @param fullName
     * @return The object
     */
    Element getElement(String fullName) throws ConnectorException;

    /**
     * Look up an object by identifier
     * @param fullName
     * @return The object
     */
    Procedure getProcedure(String fullName) throws ConnectorException;
    
    /**
     * Gets the contents of a VDB resource in binary form.
     * @param resourcePath a path returned by getVDBResourcePaths()
     * @return the binary contents of the resource in a byte[]
     * @throws ConnectorException if the operation fails
     * @since 4.3
     */
    public byte[] getBinaryVDBResource(String resourcePath) throws ConnectorException;

    /**
     * Gets the contents of a VDB resource as a String.
     * @param resourcePath a path returned by getVDBResourcePaths()
     * @return the contents of the resource as a String of characters
     * @throws ConnectorException if the operation fails
     * @since 4.3
     */
    public String getCharacterVDBResource(String resourcePath) throws ConnectorException;

    /**
     * Gets the resource paths of all the resources in the VDB. 
     * @return an array of resource paths of the resources in the VDB
     * @throws ConnectorException if the operation fails
     * @since 4.3
     */
    public String[] getVDBResourcePaths() throws ConnectorException;
}
