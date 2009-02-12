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

package com.metamatrix.connector.object.extension;

import java.util.List;

/**
 * Specifies how value objects are retrieved from results after
 * the process calls {@link IObjectSource#getObjects(IObjectCommand)}.
 * This will allow different connector implementations to specialize based on
 * the source that is being accesses.
 */
public interface IValueRetriever {
           
    /**
     * Called to convert an object into a row.  This object could represent any type of object.  It's
     * the responsibility of the implementor to determine how this object should be converted into a row. 
     * @param object is representative of one row in the result from calling {@link IObjectSource#getObjects(IObjectCommand)}
     * @param command is the executed command that the conversion is based.
     * @return a List that contains one row of data
     * @throws Exception
     * @since 4.3
     */
    List convertIntoRow(Object object, IObjectCommand command) throws Exception;

}
