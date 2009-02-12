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

import java.util.List;

/**
 * ProcedureRecord
 */
public interface ProcedureRecord extends MetadataRecord {

    /**
     * Constants for perperties stored on a ProcedureRecord 
     * @since 4.3
     */
    public interface ProcedureRecordProperties {

        String STORED_PROC_INFO_FOR_RECORD  = "storedProcInfoForRecord";  //$NON-NLS-1$
        
    }

    /**
     * Get a list of identifiers for the parameters in the procedure
     * @return a list of identifiers
     */
    List getParameterIDs();
    
    /**
     * Check if this record is for a procedure that is a function.
     * @return true if the procedure is a function
     */
    boolean isFunction();    

    /**
     * Check if this record is for a procedure that is a virtual.
     * @return true if the procedure is a virtual
     */
    boolean isVirtual();

    /**
     * Get the identifier for a resultSet in the procedure
     * @return an identifier for the resultSet.
     */    
    Object getResultSetID();
    
    /**
     * Return short indicating of PROCEDURE it is. 
     * @return short
     *
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataConstants.PROCEDURE_TYPES
     */
    short getType();
    
    int getUpdateCount();
    
}