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

import java.io.IOException;
import java.io.OutputStream;




/**
 * This generator creates the load and refresh scripts necessary to facilitate all
 * Materialized Views in a VDB.
 *  
 * @since 4.2
 */
public interface MaterializedViewScriptGenerator {
    
    /**
     * Generate the materialization truncate scripts for all materialized views in a VDB
     * for all supported RDBMS platforms and write them to the given stream. 
     * @param stream The stream that will contain all truncate scripts.
     * @param dialect The type of RDBMS for which to generated the script.
     * @throws IOException if an error occurs with the given stream or with any stream
     * used internally to help with scipt generation. 
     * @since 4.2
     */
    void generateMaterializationTruncateScript(final OutputStream stream, DatabaseDialect dialect) throws IOException;
    
    /**
     * Generate the materialization load scripts for all materialized views in a VDB
     * for the MetaMatrix platform and write them to the given stream. 
     * @param stream The stream that will contain all load scripts.
     * @throws IOException if an error occurs with the given stream or with any stream
     * used internally to help with scipt generation. 
     * @since 4.2
     */
    void generateMaterializationLoadScript(final OutputStream stream) throws IOException;
    
    /**
     * Generate the materialization rename scripts for all materialized views in a VDB
     * for all supported RDBMS platforms and write them to the given stream. 
     * @param stream The stream that will contain all rename scripts.
     * @param dialect The type of RDBMS for which to generated the script.
     * @throws IOException if an error occurs with the given stream or with any stream
     * used internally to help with scipt generation. 
     * @since 4.2
     */
    void generateMaterializationSwapScript(final OutputStream stream, DatabaseDialect dialect) throws IOException;
    
    /**
     * Generate the connection properties file for all materialized views in a VDB and 
     * write it to the given stream. 
     * @param stream The stream that will contain the connection property file.
     * @throws IOException if an error occurs with the given stream or with any stream
     * used internally to help with file generation. 
     * @since 4.2
     */
    void generateMaterializationConnectionPropFile(final OutputStream stream) throws IOException;
}
