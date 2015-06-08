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
package org.teiid.olingo.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.EmbeddedProfile;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.net.TeiidURL;
import org.teiid.odata.api.Client;
import org.teiid.odata.api.CountResponse;
import org.teiid.odata.api.OperationResponse;
import org.teiid.odata.api.ProcedureReturnType;
import org.teiid.odata.api.QueryResponse;
import org.teiid.odata.api.SQLParameter;
import org.teiid.odata.api.UpdateResponse;
import org.teiid.odbc.ODBCServerRemoteImpl;
import org.teiid.olingo.ODataPlugin;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.translator.CacheDirective;
import org.teiid.transport.LocalServerConnection;

public class LocalClient implements Client {
    static final String DELIMITER = "--" ; //$NON-NLS-1$

    private volatile VDBMetaData vdb;
    private final String vdbName;
    private final int vdbVersion;
    private ConnectionImpl connection;
    private Properties properties;

    public LocalClient(String vdbName, int vdbVersion, Properties properties) {
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.properties = properties;
    }
        
    private long getCacheTime() {
        return PropertiesUtils.getLongProperty(this.properties, Client.SKIPTOKEN_TIME, 300000L);        
    }
    
    @Override
    public Connection open() throws SQLException {
        this.connection = buildConnection(TeiidDriver.getInstance(), this.vdbName, this.vdbVersion, this.properties);
        ODBCServerRemoteImpl.setConnectionProperties(connection);
        ODBCServerRemoteImpl.setConnectionProperties(connection, this.properties);
        return this.connection;
    }

    @Override
    public void close() throws SQLException {
        if (this.connection != null) {
            this.connection.close();
        }
    }    
    
    public ConnectionImpl getConnection() {
        return this.connection;
    }
    
    public static ConnectionImpl buildConnection(TeiidDriver driver, String vdbName, int version, Properties props) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:teiid:").append(vdbName).append(".").append(version).append(";"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        if (props.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION) == null) {
            props.setProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "true"); //$NON-NLS-1$    
        }
        if (props.getProperty(EmbeddedProfile.TRANSPORT_NAME) == null) {
            props.setProperty(EmbeddedProfile.TRANSPORT_NAME, "odata");    
        }        
        if (props.getProperty(EmbeddedProfile.WAIT_FOR_LOAD) == null) {
            props.setProperty(EmbeddedProfile.WAIT_FOR_LOAD, "0"); //$NON-NLS-1$
        }        
        ConnectionImpl connection = driver.connect(sb.toString(), props);
        return connection;
    }    
    
    @Override
    public VDBMetaData getVDB() {
        if (this.vdb == null) {
            try {
                LocalServerConnection lsc = (LocalServerConnection) getConnection().getServerConnection();
                VDBMetaData vdb = lsc.getWorkContext().getVDB();
                if (vdb == null) {
                    throw new TeiidRuntimeException(ODataPlugin.Util.gs(
                            ODataPlugin.Event.TEIID16001, this.vdbName,
                            this.vdbVersion));
                }
                this.vdb = vdb;
            } catch (SQLException e) {
                throw new TeiidRuntimeException(e);
            }
        }
        return this.vdb;
    }

    @Override
    public void executeCall(String sql, List<SQLParameter> parameters, ProcedureReturnType returnType, 
            OperationResponse response) throws SQLException {
        
        LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
        final CallableStatement stmt = getConnection().prepareCall(sql);

        int i = 1;
        if (!returnType.hasResultSet()) {
            stmt.registerOutParameter(i++, returnType.getSqlType());
        }

        if (!parameters.isEmpty()) {
            for (SQLParameter param:parameters) {
                stmt.setObject(i++, param.getValue(), param.getSqlType());
            }
        }

        boolean results = stmt.execute();
        if (results) {
            final ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                if (returnType.hasResultSetBasedLob()) {
                    response.addPrimitive(rs.getObject(1));
                }
                else {
                    response.addRow(rs);
                }
            }
        }

        if (!returnType.hasResultSet()) {
            Object result = stmt.getObject(1);
            response.addPrimitive(result);
        }
    }

    @Override
    public MetadataStore getMetadataStore() {
        return getVDB().getAttachment(TransformationMetadata.class).getMetadataStore();
    }

    @Override
    public void executeSQL(Query query, List<SQLParameter> parameters,
            boolean calculateTotalSize, Integer skipOption, Integer topOption,
            String nextOption, int pageSize, final QueryResponse response)  throws SQLException {
        boolean cache = pageSize > 0; 
        if (cache) {
            CacheHint hint = new CacheHint();
            hint.setTtl(getCacheTime());
            hint.setScope(CacheDirective.Scope.USER);
            query.setCacheHint(hint);
        }
        
        boolean getCount = false; 
        getCount = calculateTotalSize;
        if (!getCount && (topOption != null || skipOption != null)) {
            if (topOption != null && skipOption != null) {
                query.setLimit(new Limit(new Constant(skipOption), new Constant(topOption)));
            }
            else if (topOption != null) {
                query.setLimit(new Limit(new Constant(0), new Constant(topOption)));
            }
        }

        String sessionId = getConnection().getServerConnection().getLogonResult().getSessionID();
        
        String nextToken = null;            
        if (nextOption != null) {
            nextToken = nextOption;
            if (cache) {
                int idx = nextOption.indexOf(DELIMITER);
                sessionId = nextOption.substring(0, idx);
                nextToken = nextOption.substring(idx+2);
                
            }
        }
        String sql = query.toString();
        if (cache) {
            sql += " /* "+ sessionId +" */"; //$NON-NLS-1$ //$NON-NLS-2$
        }
        LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
        
        final PreparedStatement stmt = getConnection().prepareStatement(sql, cache?ResultSet.TYPE_SCROLL_INSENSITIVE:ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (parameters!= null && !parameters.isEmpty()) {
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i+1, parameters.get(i).getValue(), parameters.get(i).getSqlType());
            }
        }

        final ResultSet rs = stmt.executeQuery();
                    
        //skip to the initial position
        int count = 0;
        int skipSize = 0;
        //skip based upon the skip value
        if (getCount && skipOption != null) {
            skipSize = skipOption;
        }
        //skip based upon the skipToken
        if (nextToken != null) {
            skipSize += Integer.parseInt(nextToken);
        }
        if (skipSize > 0) {
            count += skip(cache, rs, skipSize);
        }

        //determine the number of records to return
        int size = pageSize;
        int top = Integer.MAX_VALUE;
        if (getCount && topOption != null) {
            top = topOption;
            size = top; 
            if (pageSize > 0) {
                size = Math.min(pageSize, size);
            }
        } else if (size < 1) {
            size = Integer.MAX_VALUE;
        }
        
        //build the results
        for (int i = 0; i < size; i++) {
            count++;
            if (!rs.next()) {
                break;
            }
            response.addRow(rs);
        }
        
        //set the count
        if (getCount) {
            if (!cache) {
                while (rs.next()) {
                    count++;
                }
            } else {
                rs.last();
                count = rs.getRow();
            }
        }
        response.setCount(count);
        
        //set the skipToken if needed
        if (cache && response.size() == pageSize) {
            long end = skipSize + response.size();
            if (getCount) {
                if (end < Math.min(top, count)) {
                    response.setNextToken(nextToken(cache, sessionId, end));
                }
            } else if (rs.next()) {
                response.setNextToken(nextToken(cache, sessionId, end));
                //will force the entry to cache or is effectively a no-op when already cached
                rs.last();    
            }
        }
    }
    
    private String nextToken(boolean cache, String sessionid, long skip) {
        if (cache) {
            return sessionid+DELIMITER+String.valueOf(skip);
        }
        return String.valueOf(skip);
    }
    
    private int skip(boolean cache, final ResultSet rs, int skipSize)
            throws SQLException {
        int skipped = 0;
        if (!cache) {
            for (int i = 0; i < skipSize; i++) {
                skipped++;
                if (!rs.next()) {
                    break;
                }
            }
        } else {
            rs.absolute(skipSize);
        }
        return skipped;
    }

    @Override
    public CountResponse executeCount(Query query, List<SQLParameter> parameters)  throws SQLException {
        String sql = query.toString();
        LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:", sql); //$NON-NLS-1$
        final PreparedStatement stmt = getConnection().prepareStatement(sql);
        if (!parameters.isEmpty()) {
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i + 1, parameters.get(i).getValue(),
                        parameters.get(i).getSqlType());
            }
        }
        ResultSet rs = stmt.executeQuery();
        rs.next();
        final int count = rs.getInt(1);
        rs.close();
        stmt.close();
        return new CountResponse() {
            @Override
            public int getCount() {
                return count;
            }
        };
    }

    @Override
    public UpdateResponse executeUpdate(Command query, List<SQLParameter> parameters)  throws SQLException {
        String sql = query.toString();
        LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:", sql); //$NON-NLS-1$
        final PreparedStatementImpl stmt = getConnection().prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT,
                Statement.RETURN_GENERATED_KEYS);
        if (!parameters.isEmpty()) {
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i + 1, parameters.get(i).getValue(),
                        parameters.get(i).getSqlType());
            }
        }
        final int count = stmt.executeUpdate();
        final Map<String, Object> keys = getGeneratedKeys(stmt.getGeneratedKeys());
        stmt.close();
        return new UpdateResponse() {
            @Override
            public Map<String, Object> getGeneratedKeys() {
                return keys;
            }

            @Override
            public int getUpdateCount() {
                return count;
            }
        };
    }

    private Map<String, Object> getGeneratedKeys(ResultSet result)
            throws SQLException {
        if (result == null) {
            return null;
        }

        HashMap<String, Object> keys = new HashMap<String, Object>();
        ResultSetMetaData metadata = result.getMetaData();
        // now read the values
        while (result.next()) {
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                String label = metadata.getColumnLabel(i + 1);
                keys.put(label, result.getObject(i + 1));
            }
        }
        return keys;
    }

    @Override
    public String getProperty(String key) {
        return this.properties.getProperty(key);
    }

    @Override
    public String startTransaction() throws SQLException {
        getConnection().setAutoCommit(false);
        return "anyid";
    }

    @Override
    public void commit(String txnId) throws SQLException {
        getConnection().commit();
        getConnection().setAutoCommit(true);
    }

    @Override
    public void rollback(String txnId)  throws SQLException {
        getConnection().rollback();
        getConnection().setAutoCommit(true);
    }
}
