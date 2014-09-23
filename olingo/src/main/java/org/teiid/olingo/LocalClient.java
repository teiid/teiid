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
package org.teiid.olingo;

import java.io.IOException;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.core.data.PropertyImpl;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.EmbeddedProfile;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.net.TeiidURL;
import org.teiid.odbc.ODBCServerRemoteImpl;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.translator.CacheDirective;
import org.teiid.transport.LocalServerConnection;

public class LocalClient implements Client {
	private static final String BATCH_SIZE = "batch-size"; //$NON-NLS-1$
	private static final String SKIPTOKEN_TIME = "skiptoken-cache-time"; //$NON-NLS-1$
	static final String INVALID_CHARACTER_REPLACEMENT = "invalid-xml10-character-replacement"; //$NON-NLS-1$

	private volatile VDBMetaData vdb;
	private String vdbName;
	private int vdbVersion;
	private int batchSize;
	private long cacheTime;
	private String connectionString;
	private Properties connectionProperties = new Properties();
	private Properties initProperties;
	private TeiidDriver driver = TeiidDriver.getInstance();

	public LocalClient(String vdbName, int vdbVersion, Properties props) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.batchSize = PropertiesUtils.getIntProperty(props, BATCH_SIZE, BufferManagerImpl.DEFAULT_PROCESSOR_BATCH_SIZE);
		this.cacheTime = PropertiesUtils.getLongProperty(props, SKIPTOKEN_TIME, 300000L);
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:teiid:").append(this.vdbName).append(".").append(this.vdbVersion).append(";"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		this.initProperties = props;
		connectionProperties.put(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "true"); //$NON-NLS-1$
		connectionProperties.put(EmbeddedProfile.TRANSPORT_NAME, props.getProperty(EmbeddedProfile.TRANSPORT_NAME, "odata")); //$NON-NLS-1$
		connectionProperties.put(EmbeddedProfile.WAIT_FOR_LOAD, "0"); //$NON-NLS-1$
		this.connectionString = sb.toString();
	}
	
	@Override
	public VDBMetaData getVDB() {
        ConnectionImpl connection = null;
        if (this.vdb == null) {
            try {
                connection = getConnection();
                LocalServerConnection lsc = (LocalServerConnection)connection.getServerConnection();
                VDBMetaData vdb = lsc.getWorkContext().getVDB();
                if (vdb == null) {
                    throw new TeiidRuntimeException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16001, this.vdbName, this.vdbVersion));
                }
                this.vdb = vdb;
            } catch (SQLException e) {
                throw new TeiidRuntimeException(e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        
                    }
                }
            }
        }
        return this.vdb;
	}
	
	public void setDriver(TeiidDriver driver) {
		this.driver = driver;
	}

	ConnectionImpl getConnection() throws SQLException {
		ConnectionImpl connection = driver.connect(this.connectionString, connectionProperties);
		ODBCServerRemoteImpl.setConnectionProperties(connection);
		ODBCServerRemoteImpl.setConnectionProperties(connection, initProperties);
		return connection;
	}

	@Override
	public BaseResponse executeCall(String sql, List<SQLParam> parameters, SingletonPrimitiveType returnType) {
		ConnectionImpl connection = null;
		/*
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
			connection = getConnection();
			final CallableStatementImpl stmt = connection.prepareCall(sql);

			int i = 1;
			if (returnType != null && returnType.isSimple()) {
				stmt.registerOutParameter(i++, JDBCSQLTypeInfo.getSQLType(ODataTypeManager.teiidType(returnType.getFullyQualifiedName())));
			}

			if (!parameters.isEmpty()) {
				for (SQLParam param:parameters) {
					stmt.setObject(i++, param.value, param.sqlType);
				}
			}

			boolean results = stmt.execute();
			if (results) {
				final ResultSet rs = stmt.getResultSet();
                OCollection.Builder resultRows = OCollections.newBuilder((EdmComplexType)((EdmCollectionType)returnType).getItemType());
                while (rs.next()) {
                	int idx = 1;
                	List<OProperty<?>> row = new ArrayList<OProperty<?>>();
                	Iterator<EdmProperty> props = ((EdmComplexType)((EdmCollectionType)returnType).getItemType()).getProperties().iterator();
                	while (props.hasNext()) {
                		EdmProperty prop = props.next();
                		row.add(buildPropery(prop.getName(), prop.getType(), rs.getObject(idx++), invalidCharacterReplacement));
                	}
                	OComplexObject erow = OComplexObjects.create((EdmComplexType)((EdmCollectionType)returnType).getItemType(), row);
                	resultRows.add(erow);
                }
                String collectionName = returnType.getFullyQualifiedTypeName();
                collectionName = collectionName.replace("(", "_"); //$NON-NLS-1$ //$NON-NLS-2$
                collectionName = collectionName.replace(")", "_"); //$NON-NLS-1$ //$NON-NLS-2$
				return Responses.collection(resultRows.build(), null, null, null, collectionName);
			}

            if (returnType != null) {
            	Object result = stmt.getObject(1);
            	OProperty prop = buildPropery("return", returnType, result, invalidCharacterReplacement); //$NON-NLS-1$
            	return Responses.property(prop); 
            }
			return Responses.simple(EdmSimpleType.INT32, 1);
		} catch (Exception e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}
		*/
		return null;
	}

	@Override
	public MetadataStore getMetadataStore() {
		return getVDB().getAttachment(TransformationMetadata.class).getMetadataStore();
	}

	@Override
	public void executeSQL(Query query, List<SQLParam> parameters, boolean countQuery, Integer skipOption, Integer topOption, final QueryResponse respose) {
		Connection connection = null;
		try {
			boolean cache = this.batchSize > 0; 
			if (cache) {
				CacheHint hint = new CacheHint();
				hint.setTtl(this.cacheTime);
				hint.setScope(CacheDirective.Scope.USER);
				query.setCacheHint(hint);
			}
						
			String sql = query.toString();

			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$

			connection = getConnection();
			final PreparedStatement stmt = connection.prepareStatement(sql, cache?ResultSet.TYPE_SCROLL_INSENSITIVE:ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			if (parameters!= null && !parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			final ResultSet rs = stmt.executeQuery();		
			
			//skip to the initial position
			int count = 0;
			int skipSize = 0;
			//skip based upon the skip value
			if (countQuery && skipOption != null) {
				skipSize = skipOption;
			}
			//skip based upon the skipToken
			if (skipOption != null) {
				skipSize += skipOption;
			}
			if (skipSize > 0) {
				count += skip(cache, rs, skipSize);
			}

			//determine the number of records to return
			int size = batchSize;
			int top = Integer.MAX_VALUE;
			if (countQuery && topOption != null) {
				top = topOption;
				size = top; 
				if (batchSize > 0) {
					size = Math.min(batchSize, size);
				}
			} else if (size < 1) {
				size = Integer.MAX_VALUE;
			}
			
			//build the results
			for (int i = 0; i < size; i++) {
				if (!rs.next()) {
					break;
				}
				count++;
				respose.addRow(rs);
			}
			
			//set the count
			if (countQuery) {
				if (!cache) {
					while (rs.next()) {
						count++;
					}
				} else {
					rs.last();
					count = rs.getRow();
				}
			}
			respose.setCount(count);
			
			//set the skipToken if needed
			if (cache && respose.size() == this.batchSize) {
				long end = skipSize + respose.size();
				if (countQuery) {
					if (end < Math.min(top, count)) {
						respose.setNext(end);
					}
				} else if (rs.next()) {
					respose.setNext(end);
					//will force the entry to cache or is effectively a no-op when already cached
					rs.last();	
				}
			}
		} catch (Exception e) {
			throw new TeiidRuntimeException(e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}
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
	public CountResponse executeCount(Query query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
			connection = getConnection();
			final PreparedStatementImpl stmt = connection.prepareStatement(sql);
			if (!parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			ResultSet rs = stmt.executeQuery();
			rs.next();
			final int count = rs.getInt(1);
			rs.close();
			stmt.close();
			return new CountResponse() {
				@Override
				public long getCount() {
					return count;
				}
			};
		} catch (Exception e) {
			throw new TeiidRuntimeException(e);
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
			}
		}
	}

	@Override
	public UpdateResponse executeUpdate(Command query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
			connection = getConnection();
			final PreparedStatementImpl stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT, Statement.RETURN_GENERATED_KEYS);
			if (!parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
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
		} catch (Exception e) {
			throw new TeiidRuntimeException(e);
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
			}
		}
	}
	
	private Map<String, Object> getGeneratedKeys(ResultSet result) throws SQLException{
		if (result == null) {
			return null;
		}
		
		HashMap<String, Object> keys = new HashMap<String, Object>();
		ResultSetMetaData metadata = result.getMetaData();
		// now read the values
		while(result.next()) {
			for (int i = 0; i < metadata.getColumnCount(); i++) {
				String label = metadata.getColumnLabel(i+1);
				keys.put(label, result.getObject(i+1));
			}
		}
		return keys;
	}
	
	@Override
	public String getProperty(String name) {
		return this.initProperties.getProperty(name);
	}
}
