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

package com.metamatrix.modeler.core.metadata.runtime;

/**
 * ProcedureParameterRecord
 */
public interface ProcedureParameterRecord extends MetadataRecord {

    /**
     * Get the runtime type name of the parameter
     * @return column's runtime type
     */    
    String getRuntimeType();

    /**
     * Get the UUID of the datatype associated with the column
     * @return the UUID of the datatype
     */
    String getDatatypeUUID();

    /**
     * Get the default value of the parameter
     * @return parameter's default value
     */
    Object getDefaultValue();
    
    /**
     * Get the length of the parameter
     * @return parameter's length
     */
    int getLength();

    /**
     * Get the nullability of the parameter
     * @return parameter's nullability
     */
    int getNullType();

    /**
     * Get the precision of the parameter
     * @return parameter's precision
     */
    int getPrecision();

    /**
     * Get the position of the parameter
     * @return parameter's position
     */
    int getPosition();

    /**
     * Get the scale of the parameter
     * @return parameter's scale
     */
    int getScale();

    /**
     * Get the radix of the parameter
     * @return parameter's radix
     */
    int getRadix();

    /**
     * Check if the parameter is optional
     * @return true if this parameter is optional
     */
    boolean isOptional();

    /**
     * Return short indicating the type of KEY it is. 
     * @return short
     *
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataConstants.PARAMETER_TYPES
     */
    short getType();
}