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
