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

package com.metamatrix.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.client.MetadataResult;
import com.metamatrix.dqp.metadata.ResultsMetadataConstants;

/**
 * This metadata provider starts with just column names and types (provided in the response)
 * but must lazily load the rest of the metadata when necessary.
 */
public class DeferredMetadataProvider extends AbstractMetadataProvider {
    private StaticMetadataProvider staticProvider;

    private MMStatement statement;
    private long requestID;

    DeferredMetadataProvider() {        
    }
    
    public static DeferredMetadataProvider createWithInitialData(String[] columnNames, String[] columnTypes, MMStatement statement, long requestID) {
        if(columnNames == null || columnTypes == null || columnNames.length != columnTypes.length) {
            Object[] params = new Object[] { 
                StringUtil.toString(columnNames), StringUtil.toString(columnTypes)
            };
            throw new IllegalArgumentException(JDBCPlugin.Util.getString("DeferredMetadataProvider.Invalid_data", params)); //$NON-NLS-1$
        }
        
        DeferredMetadataProvider provider = null;
        provider = new DeferredMetadataProvider();    
        provider.setDeferredLookupAttributes(statement, requestID);
        provider.loadPartialMetadata(columnNames, columnTypes);        
        return provider;    
    }
    
    private void setDeferredLookupAttributes(MMStatement statement, long requestID) {
        this.statement = statement;
        this.requestID = requestID;
    }
    
    private void loadPartialMetadata(String[] columnNames, String[] columnTypes) {
        Map[] columnMetadata = new Map[columnNames.length];
        for(int i=0; i<columnNames.length; i++) {
            columnMetadata[i] = new HashMap();
            columnMetadata[i].put(ResultsMetadataConstants.ELEMENT_NAME, columnNames[i]);
            columnMetadata[i].put(ResultsMetadataConstants.DATA_TYPE, columnTypes[i]);
        }
        
        this.staticProvider = StaticMetadataProvider.createWithData(columnMetadata, -1);    
    }

    private void loadFullMetadata() throws SQLException {
    	MetadataResult results;
		try {
			results = this.statement.getDQP().getMetadata(this.requestID);
		} catch (MetaMatrixComponentException e) {
			throw MMSQLException.create(e);
		} catch (MetaMatrixProcessingException e) {
			throw MMSQLException.create(e);
		}
        this.staticProvider = StaticMetadataProvider.createWithData(results.getColumnMetadata(), results.getParameterCount());
    }

    public int getColumnCount() throws SQLException {
        return staticProvider.getColumnCount();
    }

    public Object getValue(int columnIndex, Integer metadataPropertyKey) throws SQLException {
        Object value = staticProvider.getValue(columnIndex, metadataPropertyKey);
        
        if(value == null) {
            loadFullMetadata();
            value = staticProvider.getValue(columnIndex, metadataPropertyKey);          
        }
        
        return value;
    }

    public int getParameterCount() throws SQLException {
        int count = staticProvider.getParameterCount();
        
        if(count < 0) {
            loadFullMetadata();
            count = staticProvider.getParameterCount();          
        }
        
        return count;
    }

}
