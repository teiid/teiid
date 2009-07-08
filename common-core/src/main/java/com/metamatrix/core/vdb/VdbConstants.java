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

package com.metamatrix.core.vdb;



/**
 * Constants used for VDB processing.
 */
public interface VdbConstants {

    public static final String DEF_FILE_NAME = "ConfigurationInfo.def"; // !!! DO NOT CHANGE VALUE as this would cause problems with existing VDBs having DEF files !!! //$NON-NLS-1$
    public static final String DATA_ROLES_FILE = "dataroles.xml"; //$NON-NLS-1$
    public static final String VDB_DEF_FILE_EXTENSION = ".def"; //$NON-NLS-1$
    public static final String VDB_ARCHIVE_EXTENSION = ".vdb"; //$NON-NLS-1$
    public static final String MATERIALIZATION_MODEL_NAME = "MaterializationModel"; //$NON-NLS-1$
    public static final String MATERIALIZATION_MODEL_FILE_SUFFIX = ".xmi"; //$NON-NLS-1$
    public static final String MANIFEST_MODEL_NAME = "MetaMatrix-VdbManifestModel.xmi"; //$NON-NLS-1$
    public static final String WSDL_FILENAME = "MetaMatrixDataServices.wsdl"; //$NON-NLS-1$
    public final static String INDEX_EXT        = ".INDEX";     //$NON-NLS-1$
    public final static String SEARCH_INDEX_EXT = ".SEARCH_INDEX";     //$NON-NLS-1$
    public final static String MODEL_EXT = ".xmi";     //$NON-NLS-1$
    
    /**
     * These are virtual database status.
     */
    final public static class VDB_STATUS {
        public static final short INCOMPLETE = 1;
        public static final short INACTIVE = 2;
        public static final short ACTIVE = 3;
        public static final short DELETED = 4;
    }

	public static final String VDB = ".vdb"; //$NON-NLS-1$
	public static final String DEF = ".def"; //$NON-NLS-1$    
    
}
