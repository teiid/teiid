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

package com.metamatrix.server.admin.api;

import java.io.InputStream;
import java.io.Serializable;

import com.metamatrix.vdb.materialization.DatabaseDialect;


/** 
 * Contains all information nessecary to save the scripts that will be run
 * by a MetaMatrix utility to load or refresh the data resident in a
 * Materialized View. 
 * <p>
 * Users should get the contents of each file and save each with the
 * filename associated with it.</p>
 * <p>
 * <b>NOTE</b>: <i>Users should close the </i><code>InputStream</code><i> representing
 * the file contents when finished.</i></p>
 * @since 4.2
 */
public interface MaterializationLoadScripts extends Serializable {
    
    public interface DatabaseType {
        static final String ORACLE = DatabaseDialect.ORACLE.getType(); 
        static final String DB2 = DatabaseDialect.DB2.getType(); 
        static final String SQL_SERVER = DatabaseDialect.SQL_SERVER.getType(); 
    }
    
    /**
     * Get a stream of the generated truncate script.
     * 
     * <p><b>NOTE</b>: Client is responsible for closing the stream.<p>
     * @return Truncate script.
     * @since 4.2
     */
    InputStream getTruncateScriptFile(); 
    
    /**
     * Get the file name the the truncate script should be named. 
     * @return Truncate script file name.
     * @since 4.2
     */
    String getTruncateScriptFileName();
    
    /**
     * Get a stream of the generated load script.
     * 
     * <p><b>NOTE</b>: Client is responsible for closing the stream.<p>
     * @return Load script
     * @since 4.2
     */
    InputStream getLoadScriptFile(); 
    
    /**
     * Get the file name the the load script should be named. 
     * @return Load script file name.
     * @since 4.2
     */
    String getLoadScriptFileName();

    /**
     * Get a stream of the generated swap script.
     * 
     * <p><b>NOTE</b>: Client is responsible for closing the stream.<p>
     * @return 
     * @since 4.2
     */
    InputStream getSwapScriptFile(); 
    
    /**
     * Get the file name the the swap script should be named. 
     * @return Swap script file name.
     * @since 4.2
     */
    String getSwapScriptFileName();

    /**
     * Get a stream for the generated DDL script.
     * 
     * <p><b>NOTE</b>: Client is responsible for closing the stream.<p>
     * @return DDL script.
     * @since 4.2
     */
    InputStream getCreateScriptFile(); 
    
    /**
     * Get the file name the the DDL script should be named. 
     * @return DDL script file name.
     * @since 4.2
     */
    String getCreateScriptFileName();
    
    /** 
     * Get the contents of the connection properties file.
     * @return Returns the connection properties file contents.
     * @since 4.2
     */
    InputStream getConnectionPropsFileContents();

    /** 
     * Get the file name the connection properties should be named.
     * @return Returns the connection properties file name.
     * @since 4.2
     */
    String getConnectionPropsFileName();
}
