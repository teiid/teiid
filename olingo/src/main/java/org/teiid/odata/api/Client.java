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
package org.teiid.odata.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;

public interface Client {
    public static final String INVALID_CHARACTER_REPLACEMENT = "invalid-xml10-character-replacement"; //$NON-NLS-1$
    public static final String BATCH_SIZE = "batch-size"; //$NON-NLS-1$
    public static final String SKIPTOKEN_TIME = "skiptoken-cache-time"; //$NON-NLS-1$
    public static final String CHARSET = "charset"; //$NON-NLS-1$
    
    VDBMetaData getVDB();

    MetadataStore getMetadataStore();

    void executeCall(String sql, List<SQLParameter> sqlParams, ProcedureReturnType returnType,
            OperationResponse response) throws SQLException;

    void executeSQL(Query query, List<SQLParameter> parameters,
            boolean calculateTotalSize, Integer skip, Integer top, String nextOption, int pageSize,
            QueryResponse response) throws SQLException;

    CountResponse executeCount(Query query, List<SQLParameter> parameters) throws SQLException;

    UpdateResponse executeUpdate(Command command, List<SQLParameter> parameters) throws SQLException;
        
    String startTransaction() throws SQLException;

    void commit(String txnId) throws SQLException;
    
    void rollback(String txnId) throws SQLException;
    
    String getProperty(String name);
    
    Connection open() throws SQLException;
    
    void close() throws SQLException;
}