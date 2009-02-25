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

import java.util.List;

import org.teiid.connector.api.ConnectorException;


/**
 * Represents a procedure parameter in the runtime metadata.
 */
public interface Parameter extends MetadataObject, TypeModel {

    public static final int IN = 0;
    public static final int OUT = 1;
    public static final int INOUT = 2;
    public static final int RETURN = 3;
    public static final int RESULT_SET = 4;

    /**
     * Index of the parameter in the procedure.  If the parameter has 
     * no index, then the index will be returned as -1.    
     * @return Index of the parameter
     */
    int getIndex() throws ConnectorException;
    
    /**
     * Get direction of the parameter, as specified by direction constants.
     * @return Direction constant
     * @see #IN
     * @see #OUT
     * @see #INOUT
     * @see #RETURN
     * @see #RESULT_SET
     */
    int getDirection() throws ConnectorException;
    
    /**
     * Get the parent
     * @return Parent
     */
    Procedure getParent() throws ConnectorException;
    
    List<Element> getChildren() throws ConnectorException;
    
}
