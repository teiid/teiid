/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.odata.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidProcessingException;
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

    Connection open() throws SQLException, TeiidProcessingException;

    void close() throws SQLException;
}