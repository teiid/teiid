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

import java.util.List;

/**
 * <p>Instances of this interface represent Procedures in a Model.  The values of a Procedure are analogous to a Stored Procedure or Function in a database.</p> 
 */
public interface Procedure extends MetadataObject {
/**
 * Return the path to the procedure.
 *  @return String 
 */
    String getPath();
/**
 * Return the procedure description.
 * @return String 
 */
    String getDescription();
/**
 * Return the alias.
 *  @return String alias
 */
    String getAlias();
/**
 * Returns an ordered list  of type <code>ProcedureParameter</code> that represent all the parameters the procedure has.
 * @return List of ProcedureParameters
 * @see ProcedureParameter
 */
    List getParameters();
/**
 * Returns a boolean indicating if this procedure returns a result set.
 * @return boolean is true if a result will be returned
 */
    boolean returnsResults();
    
/**
 * Returns the queryPlan.
 * @return String
 */
public String getQueryPlan();
        
/**
 * Return short indicating the type of procedure.
 * @return short
 *   
 * @see com.metamatrix.metadata.runtime.api.MetadataConstants.PROCEDURE_TYPES
 */
    short getProcedureType();
}

