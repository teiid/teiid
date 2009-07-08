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

package com.metamatrix.metadata.runtime.api;


/**
 * <p>Instances of this interface represent Parameters for a Procedure.  A Procedure can have various types of parameters.  The types are as follows:
 *  <ul>
 *  <li>IN - Input parameter</li>
 *  <li>OUT - Output parameter</li>
 *  <li>INOUT - Input-Output parameter</li>
 *  <li>RETURN VALUE - a return value</li>
 *  <li>RESULT SET(S) - one or more nested result sets</li>
 *  </ul>
 * </p> 
 */
public interface ProcedureParameter  {
/**
 * Returns the <code>DataType</code> this parameter will be represented as.
 * @return DataType 
 */
    DataType getDataType();
/**
 * Return short indicating the type of parameter.
 * @return short
 *
 * @see com.metamatrix.metadata.runtime.api.MetadataConstants.PARAMETER_TYPES
 */
    short getParameterType();
/**
 * Returns a boolean indicating if this parameter is optional.
 * @return boolean true when the parameter is optional 
 */
    boolean isOptional();
/**
 * Returns the order of parameter in relation to the other parameters that are of the same type for its procedure.
 *  @return int postion
 */
    int getPosition();
    int getResultSetPosition();
    String getName();
/**
 * Returns the default value of parameter if it is optional.
 *  @return int postion
 */
    String getDefaultValue();
/**
 * Returns the procID.
 * @return ProcedureID
 */
    ProcedureID getProcID();
    
    
}

