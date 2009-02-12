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

package com.metamatrix.vdb.materialization;

import com.metamatrix.core.util.StringUtil;


/** 
 * @since 4.2
 */
public class ScriptType {
    public static final String MATERIALIZATION_SCRIPT_NAME_SEPARATOR = "_"; //$NON-NLS-1$
    
    public static final String MATERIALIZATION_MODEL_FILE_PREFIX = "MaterializationModel"; //$NON-NLS-1$
    public static final String MATERIALIZATION_CREATE_SCRIPT_FILE_PREFIX = "Create"; //$NON-NLS-1$
    public static final String MATERIALIZATION_LOAD_SCRIPT_FILE_PREFIX = "Load"; //$NON-NLS-1$
    public static final String MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX = "Truncate"; //$NON-NLS-1$
    public static final String MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX = "Swap"; //$NON-NLS-1$
    public static final String MATERIALIZATION_SCRIPT_FILE_SUFFIX = ".ddl"; //$NON-NLS-1$

    public static final String MATERIALIZATION_CONNECTION_PROP_FILE_PREFIX = "Connection"; //$NON-NLS-1$
    public static final String MATERIALIZATION_CONNECTION_PROP_FILE_SUFFIX = ".properties"; //$NON-NLS-1$
    public static final String MATERIALIZATION_SCRIPTS_LOG_FILE_SUFFIX = ".log"; //$NON-NLS-1$
    
    /**
     * Utility functions to create the needed file name patterns
     */
    public static String createScriptFileName(String vdbName, String vdbVersion) {
        return buildRuntimeScriptName(vdbName, vdbVersion,MATERIALIZATION_CREATE_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }
    public static String createScriptFileName(DatabaseDialect dbDialect, String vdbName) {
        return buildVDBScriptName(dbDialect.getType(), vdbName, MATERIALIZATION_CREATE_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }
    
    public static String loadScriptFileName(String vdbName, String vdbVersion) {
        return buildRuntimeScriptName(vdbName, vdbVersion,MATERIALIZATION_LOAD_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }
    public static String loadScriptFileName(DatabaseDialect dbDialect, String vdbName) {
        return buildVDBScriptName(dbDialect.getType(), vdbName, MATERIALIZATION_LOAD_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }
    
    public static String swapScriptFileName(String vdbName, String vdbVersion) {
        return buildRuntimeScriptName(vdbName, vdbVersion,MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }
    public static String swapScriptFileName(DatabaseDialect dbDialect, String vdbName) {
        return buildVDBScriptName(dbDialect.getType(), vdbName, MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }    
    
    public static String truncateScriptFileName(String vdbName, String vdbVersion) {
        return buildRuntimeScriptName(vdbName, vdbVersion,MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }
    public static String truncateScriptFileName(DatabaseDialect dbDialect, String vdbName) {
        return buildVDBScriptName(dbDialect.getType(), vdbName, MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX, MATERIALIZATION_SCRIPT_FILE_SUFFIX);
    }
    
    public static String connectionPropertyFileName(String vdbName, String vdbVersion) {
        return buildRuntimeScriptName(vdbName, vdbVersion, MATERIALIZATION_CONNECTION_PROP_FILE_PREFIX, MATERIALIZATION_CONNECTION_PROP_FILE_SUFFIX);
    }
    public static String logFileName(String vdbName, String vdbVersion) {
        StringBuffer sb = new StringBuffer();
        sb.append(vdbName)
        	.append(MATERIALIZATION_SCRIPT_NAME_SEPARATOR)
        	.append(vdbVersion)
        	.append(MATERIALIZATION_SCRIPTS_LOG_FILE_SUFFIX);              
        return sb.toString();
    }

    public static boolean isDDLScript(String name) {
        return StringUtil.endsWithIgnoreCase(name, ScriptType.MATERIALIZATION_SCRIPT_FILE_SUFFIX);        
    }
    
    public static boolean isMaterializationScript(String name) {
        return StringUtil.indexOfIgnoreCase(name, MATERIALIZATION_MODEL_FILE_PREFIX) >= 0;        
    }
    public static boolean isCreateScript(String name) {
        return StringUtil.indexOfIgnoreCase(name, MATERIALIZATION_CREATE_SCRIPT_FILE_PREFIX) >= 0;        
    }
    
    public static boolean isLoadScript(String name) {
        return StringUtil.indexOfIgnoreCase(name, MATERIALIZATION_LOAD_SCRIPT_FILE_PREFIX) >= 0;
    }
    public static boolean isSwapScript(String name) {
        return StringUtil.indexOfIgnoreCase(name, MATERIALIZATION_SWAP_SCRIPT_FILE_PREFIX) >= 0;
    }
    public static boolean isTruncateScript(String name) {
        return StringUtil.indexOfIgnoreCase(name, MATERIALIZATION_TRUNCATE_SCRIPT_FILE_PREFIX) >= 0;
    }
        
    private static String buildRuntimeScriptName(String vdbName, String version, String type, String suffix) {
        StringBuffer sb = new StringBuffer();
        sb.append(vdbName)
        	.append(MATERIALIZATION_SCRIPT_NAME_SEPARATOR)
        	.append(version)
        	.append(MATERIALIZATION_SCRIPT_NAME_SEPARATOR)
        	.append(type)
        	.append(suffix);              
        return sb.toString();
    }      
    
    private static String buildVDBScriptName(String dbType, String vdbName, String type, String suffix) {
        StringBuffer sb = new StringBuffer();
        sb.append(dbType).append(MATERIALIZATION_SCRIPT_NAME_SEPARATOR)
        	.append(vdbName)
        	.append(MATERIALIZATION_SCRIPT_NAME_SEPARATOR)
        	.append(type)
        	.append(suffix);              
        return sb.toString();
    }      
}
