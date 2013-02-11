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

package org.teiid.metadata;

import org.teiid.translator.TranslatorException;

/**
 * Helper methods that can be used to access runtime metadata.
 */
public interface RuntimeMetadata {

    /**
     * Look up an object by identifier
     * @param fullName
     * @return The object or throws a {@link TranslatorException} if the object cannot be found
     */
    Table getTable(String fullName) throws TranslatorException;

    /**
     * Look up an object by identifier
     * @return The object or throws a {@link TranslatorException} if the object cannot be found
     */
    Table getTable(String schema, String name) throws TranslatorException;

    /**
     * Look up an object by identifier
     * @param fullName
     * @return The object or throws a {@link TranslatorException} if the object cannot be found
     */
    Column getColumn(String fullName) throws TranslatorException;

    /**
     * Look up an object by identifier
     * @return The object or throws a {@link TranslatorException} if the object cannot be found
     */
    Column getColumn(String schema, String table, String name) throws TranslatorException;

    /**
     * Look up an object by identifier
     * @param fullName
     * @return The object or throws a {@link TranslatorException} if the object cannot be found
     */
    Procedure getProcedure(String fullName) throws TranslatorException;

    /**
     * Look up an object by identifier
     * @return The object or throws a {@link TranslatorException} if the object cannot be found
     */
    Procedure getProcedure(String schema, String name) throws TranslatorException;
    
    /**
     * Gets the contents of a VDB resource in binary form.
     * @param resourcePath a path returned by getVDBResourcePaths()
     * @return the binary contents of the resource in a byte[]
     * @throws TranslatorException if the operation fails
     * @since 4.3
     */
    public byte[] getBinaryVDBResource(String resourcePath) throws TranslatorException;

    /**
     * Gets the contents of a VDB resource as a String.
     * @param resourcePath a path returned by getVDBResourcePaths()
     * @return the contents of the resource as a String of characters
     * @throws TranslatorException if the operation fails
     * @since 4.3
     */
    public String getCharacterVDBResource(String resourcePath) throws TranslatorException;

    /**
     * Gets the resource paths of all the resources in the VDB. 
     * @return an array of resource paths of the resources in the VDB
     * @throws TranslatorException if the operation fails
     * @since 4.3
     */
    public String[] getVDBResourcePaths() throws TranslatorException;
}
