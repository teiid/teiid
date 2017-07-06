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
import java.util.StringTokenizer;

import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.LocalProfile;
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
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.translator.CacheDirective;
import org.teiid.transport.LocalServerConnection;

public class LocalClient implements Client {
    static final String DELIMITER = "," ; //$NON-NLS-1$

    private volatile VDBMetaData vdb;
    private final String vdbName;
    private final String vdbVersion;
    private ConnectionImpl connection;
    private Properties properties;

    public LocalClient(String vdbName, String vdbVersion, Properties properties) {
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.properties = properties;
    }
        
    private long getCacheTime() {
        return PropertiesUtils.getLongProperty(this.properties, Client.SKIPTOKEN_TIME, 300000L);        
    }
    
    @Override
    public Connection open() throws SQLException, TeiidProcessingException {
        this.connection = buildConnection(TeiidDriver.getInstance(), this.vdbName, this.vdbVersion, this.properties);
        ODBCServerRemoteImpl.setConnectionProperties(connection);
        ODBCServerRemoteImpl.setConnectionProperties(connection, this.properties);
        getVDBInternal();
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
    
    public static ConnectionImpl buildConnection(TeiidDriver driver, String vdbName, String version, Properties props) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:teiid:").append(vdbName); //$NON-NLS-1$ 
        if (version != null) {
        	sb.append(".").append(version); //$NON-NLS-1$
        }
        sb.append(";"); //$NON-NLS-1$ 
        
        if (props.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION) == null) {
            props.setProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "true"); //$NON-NLS-1$    
        }
        if (props.getProperty(LocalProfile.TRANSPORT_NAME) == null) {
            props.setProperty(LocalProfile.TRANSPORT_NAME, "odata");    
        }        
        if (props.getProperty(LocalProfile.WAIT_FOR_LOAD) == null) {
            props.setProperty(LocalProfile.WAIT_FOR_LOAD, "0"); //$NON-NLS-1$
        }        
        ConnectionImpl connection = driver.connect(sb.toString(), props);
        return connection;
    }    
    
    @Override
    public VDBMetaData getVDB() {
        try {
			return getVDBInternal();
        } catch (TeiidProcessingException e) {
        	throw new TeiidRuntimeException(e);
		} catch (SQLException e) {
			throw new TeiidRuntimeException(e);
		}
    }

	private VDBMetaData getVDBInternal() throws SQLException, TeiidProcessingException {
		if (this.vdb == null) {
            LocalServerConnection lsc = (LocalServerConnection) getConnection().getServerConnection();
            vdb = lsc.getWorkContext().getVDB();
            if (vdb == null) {
                throw new TeiidRuntimeException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID16001, this.vdbName,
                        this.vdbVersion));
            }
            this.vdb = vdb;
        }
		if (vdb.getStatus() != Status.ACTIVE) {
			throw new TeiidProcessingException(QueryPlugin.Event.TEIID31099, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31099, vdb, vdb.getStatus()));
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
                response.addRow(rs);
            }
        }

        if (!returnType.hasResultSet()) {
            Object result = stmt.getObject(1);
            response.setReturnValue(result);
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
        boolean skipAndTopApplied = false;
        if (!getCount && (topOption != null || skipOption != null)) {
            query.setLimit(new Limit(skipOption!=null?new Constant(skipOption):null, 
                    topOption!=null?new Constant(topOption):null));
            skipAndTopApplied=true;
        }

        String sessionId = getConnection().getServerConnection().getLogonResult().getSessionID();
        
        String nextToken = null;
        Integer savedEntityCount = null;
        if (nextOption != null) {
            nextToken = nextOption;
            if (cache) {
                StringTokenizer st = new StringTokenizer(nextOption, DELIMITER);
                sessionId = st.nextToken();
                nextToken = st.nextToken();
                if (st.hasMoreTokens()) {
                    savedEntityCount = Integer.parseInt(st.nextToken());
                }
            }
            getCount = false; // the URL might have $count=true, but ignore it.
        }
        String sql = query.toString();
        if (cache) {
            sql += " /* "+ sessionId +" */"; //$NON-NLS-1$ //$NON-NLS-2$
        }
        LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
        
        final PreparedStatement stmt = getConnection().prepareStatement(sql, 
                cache?ResultSet.TYPE_SCROLL_INSENSITIVE:ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_READ_ONLY);
        if (parameters!= null && !parameters.isEmpty()) {
            List<Reference> references = ReferenceCollectorVisitor.getReferences(query);
            for (int i = 0; i < references.size(); i++) {
                int index = references.get(i).getIndex();
                stmt.setObject(i+1, parameters.get(index).getValue(), parameters.get(index).getSqlType());
            }
        }

        final ResultSet rs = stmt.executeQuery();
                    
        //skip to the initial position
        int count = 0;
        int entityCount = 0;
        int skipSize = 0;
        
        //skip based upon the skip value
        if (nextToken == null && skipOption != null && !skipAndTopApplied) {            
            if (skipOption > 0) {                
                int s = skipEntities(rs, skipOption);
                count += s;
                entityCount = s;
                skipSize = count;
            }                        
        }
        
        //skip based upon the skipToken
        if (nextToken != null) {
            skipSize += Integer.parseInt(nextToken);
            if (skipSize > 0) {
                count += skip(cache, rs, skipSize);
            }            
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
        int i = 0;
        int nextCount = count;
        while(true) {            
            if (!rs.next()) {
                break;
            }
            count++;
            i++;
            entityCount++;
            if (i > size) {
                break;
            }
            nextCount++;
            response.addRow(rs);
        }
        
        //set the count
        if (getCount) {
            while (rs.next()) {
                count++;
                entityCount++;
            }
        }
        if (savedEntityCount != null) {
            response.setCount(savedEntityCount);
        } else {
            response.setCount(entityCount);
        }
        
        //set the skipToken if needed
        if (cache && response.size() == pageSize) {
            long end = nextCount;
            if (getCount) {
                if (end < Math.min(top, count)) {
                    response.setNextToken(nextToken(cache, sessionId, end, entityCount));
                }
            } else if (count != nextCount){
                response.setNextToken(nextToken(cache, sessionId, end, null));
                //will force the entry to cache or is effectively a no-op when already cached
                rs.last();    
            }
        }
    }
    
    private String nextToken(boolean cache, String sessionid, long skip, Integer entityCount) {
        if (cache) {
            String token = sessionid+DELIMITER+String.valueOf(skip);
            if (entityCount != null) {
                token = token+DELIMITER+String.valueOf(entityCount);
            }
            return token;
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
            skipped = skipSize;
            rs.absolute(skipSize);
        }
        return skipped;
    }
    
    private int skipEntities(final ResultSet rs, int skipEntities)
            throws SQLException {
        int skipped = 0;
        while (rs.next()) {
            skipped++;
            if (skipped == skipEntities) {
                break;
            }
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
