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

package org.teiid.adminapi;

import java.util.Collection;
import java.util.Date;

/**
 * Represents a Virtual Database in the MetaMatrix system.
 * <br>A VDB has a name and a version.</br>
 * 
 * <p>The identifier pattern for a VDB is <CODE>"name<{@link #DELIMITER_CHAR}>version"</CODE>, 
 * where the name of the VDB and its version represent its unique identifier in the MetaMatrix system.
 * There are no spaces allowed in a given VDB name, and VDB name must start with a letter. 
 * A version number is automatically assigned to a VDB when it is deployed into 
 * a system. A VDB is uniquely identified by <CODE>"name<{@link #DELIMITER_CHAR}>version"</CODE>. 
 * For example: <CODE>"Accounts<{@link #DELIMITER_CHAR}>1"</CODE>, <CODE>"UnifiedSales<{@link #DELIMITER_CHAR}>4</CODE>" etc. 
 * </p>
 * 
 * @since 4.3
 */
public interface VDB extends
                    AdminObject {

    /** 
     * Constant to denote the latest version of a VDB located
     * at a given repository location. Used when deploying a
     * VDB to the MetaMatrix Server from the server repository.
     */
    public static final String SERVER_REPOSITORY_LATEST_VERSION = "LATEST"; //$NON-NLS-1$
    
    /**
     * Incomplete (if import does not have all the connector bindings)
     * 
     * @since 4.3
     */
    public static final int INCOMPLETE = 1;
    /**
     * Inactive VDB (can edit connector binding)
     * 
     * @since 4.3
     */
    public static final int INACTIVE = 2;
    /**
     * Active VDB
     * 
     * @since 4.3
     */
    public static final int ACTIVE = 3;
    /**
     * Mark VDB for Deletion
     * 
     * @since 4.3
     */
    public static final int DELETED = 4;

    /**
     * @return date the VDB was versioned
     */
    public Date getVersionedDate();

    /**
     * @return user that versioned the VDB
     */
    public String getVersionedBy();

    /**
     * @return Collection of MMModels
     */
    public Collection getModels();

    /**
     * @return the status
     */
    public int getState();

    /**
     * @return the status
     */
    public String getStateAsString();

    /**
     * @return the VDB version
     */
    public String getVDBVersion();
    
    /**
     * Check to see if this VDB has any Materialized Models
     * 
     * @return true or false 
     * @since 4.3
     */
    public boolean hasMaterializedViews();
    
    /**
     * Check to see if this VDB contains a WSDL. 
     * @return true if it contains a WSDL.
     * @since 5.5.3
     */
    public boolean hasWSDL();

}
