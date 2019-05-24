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

package org.teiid.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.teiid.client.metadata.MetadataResult;
import org.teiid.client.metadata.ResultsMetadataConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.StringUtil;


/**
 * This metadata provider starts with just column names and types (provided in the response)
 * but must lazily load the rest of the metadata when necessary.
 */
public class DeferredMetadataProvider extends MetadataProvider {
    private StatementImpl statement;
    private long requestID;
    private boolean loaded;

    public DeferredMetadataProvider(String[] columnNames, String[] columnTypes, StatementImpl statement, long requestID) {
        super(loadPartialMetadata(columnNames, columnTypes));
        this.statement = statement;
        this.requestID = requestID;
    }

    static Map<Integer, String>[] loadPartialMetadata(String[] columnNames, String[] columnTypes) {
        if(columnNames == null || columnTypes == null || columnNames.length != columnTypes.length) {
            Object[] params = new Object[] {
                StringUtil.toString(columnNames), StringUtil.toString(columnTypes)
            };
            throw new IllegalArgumentException(JDBCPlugin.Util.getString("DeferredMetadataProvider.Invalid_data", params)); //$NON-NLS-1$
        }
        Map<Integer, String>[] columnMetadata = new Map[columnNames.length];
        for(int i=0; i<columnNames.length; i++) {
            columnMetadata[i] = new HashMap<Integer, String>();
            columnMetadata[i].put(ResultsMetadataConstants.ELEMENT_LABEL, columnNames[i]);
            columnMetadata[i].put(ResultsMetadataConstants.DATA_TYPE, columnTypes[i]);
        }
        return columnMetadata;
    }

    private void loadFullMetadata() throws SQLException {
        MetadataResult results;
        try {
            results = this.statement.getDQP().getMetadata(this.requestID);
        } catch (TeiidComponentException e) {
            throw TeiidSQLException.create(e);
        } catch (TeiidProcessingException e) {
            throw TeiidSQLException.create(e);
        }
        this.metadata = results.getColumnMetadata();
    }

    @Override
    public Object getValue(int columnIndex, Integer metadataPropertyKey) throws SQLException {
        if(!loaded && !(metadataPropertyKey == ResultsMetadataConstants.ELEMENT_LABEL || metadataPropertyKey == ResultsMetadataConstants.DATA_TYPE)) {
            loadFullMetadata();
            loaded = true;
        }

        return super.getValue(columnIndex, metadataPropertyKey);
    }

}
