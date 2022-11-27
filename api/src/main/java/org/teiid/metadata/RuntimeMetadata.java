/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
