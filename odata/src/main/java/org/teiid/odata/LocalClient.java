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
package org.teiid.odata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.odata4j.core.OCollection;
import org.odata4j.core.OCollections;
import org.odata4j.core.OComplexObject;
import org.odata4j.core.OComplexObjects;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmCollectionType;
import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.ServerErrorException;
import org.odata4j.producer.BaseResponse;
import org.odata4j.producer.CountResponse;
import org.odata4j.producer.Responses;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.CallableStatementImpl;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.EmbeddedProfile;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.net.TeiidURL;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.odata.ODataEntitySchemaBuilder;
import org.teiid.translator.odata.ODataTypeManager;
import org.teiid.transport.LocalServerConnection;

public class LocalClient implements Client {
	private static final String BATCH_SIZE = "batch-size"; //$NON-NLS-1$
	private static final String SKIPTOKEN_TIME = "skiptoken-cache-time"; //$NON-NLS-1$

	private MetadataStore metadataStore;
	private String vdbName;
	private int vdbVersion;
	private int batchSize;
	private long cacheTime;
	private String transportName;
	private String connectionString;
	private EdmDataServices edmMetaData;
	private long lastLookup = -1L;
	private TeiidDriver driver = TeiidDriver.getInstance();

	public LocalClient(String vdbName, int vdbVersion, Properties props) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.batchSize = PropertiesUtils.getIntProperty(props, BATCH_SIZE, BufferManagerImpl.DEFAULT_PROCESSOR_BATCH_SIZE);
		this.cacheTime = PropertiesUtils.getLongProperty(props, SKIPTOKEN_TIME, 300000L);
		this.transportName = props.getProperty(EmbeddedProfile.TRANSPORT_NAME, "odata"); //$NON-NLS-1$
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:teiid:").append(this.vdbName).append(".").append(this.vdbVersion).append(";"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		sb.append(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION+"=true;"); //$NON-NLS-1$
		sb.append(EmbeddedProfile.TRANSPORT_NAME).append("=").append(this.transportName).append(";"); //$NON-NLS-1$ //$NON-NLS-2$
		sb.append(EmbeddedProfile.WAIT_FOR_LOAD).append("=").append("0;"); //$NON-NLS-1$ //$NON-NLS-2$
		this.connectionString = sb.toString();
	}

	@Override
	public String getVDBName() {
		return this.vdbName;
	}

	@Override
	public int getVDBVersion() {
		return this.vdbVersion;
	}
	
	public void setDriver(TeiidDriver driver) {
		this.driver = driver;
	}

	private ConnectionImpl getConnection() throws SQLException {
		return driver.connect(this.connectionString, null);
	}

	@Override
	public BaseResponse executeCall(String sql, List<SQLParam> parameters, EdmType returnType) {
		ConnectionImpl connection = null;
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$
			connection = getConnection();
			final CallableStatementImpl stmt = connection.prepareCall(sql);

			int i = 1;
			if (returnType != null && returnType.isSimple()) {
				stmt.registerOutParameter(i++, JDBCSQLTypeInfo.getSQLType(ODataTypeManager.teiidType(returnType.getFullyQualifiedTypeName())));
			}

			if (!parameters.isEmpty()) {
				for (SQLParam param:parameters) {
					stmt.setObject(i++, param.value, param.sqlType);
				}
			}

			boolean results = stmt.execute();
			if (results) {
				final ResultSet rs = stmt.getResultSet();
                OCollection.Builder resultRows = OCollections.newBuilder(returnType);
                while (rs.next()) {
                	int idx = 1;
                	List<OProperty<?>> row = new ArrayList<OProperty<?>>();
                	Iterator<EdmProperty> props = ((EdmComplexType)((EdmCollectionType)returnType).getItemType()).getProperties().iterator();
                	while (props.hasNext()) {
                		EdmProperty prop = props.next();
                		row.add(OProperties.simple(prop.getName(), rs.getObject(idx++)));
                	}
                	OComplexObject erow = OComplexObjects.create((EdmComplexType)((EdmCollectionType)returnType).getItemType(), row);
                	resultRows.add(erow);
                }
                String collectionName = returnType.getFullyQualifiedTypeName();
                collectionName = collectionName.replace("(", "_"); //$NON-NLS-1$ //$NON-NLS-2$
                collectionName = collectionName.replace(")", "_"); //$NON-NLS-1$ //$NON-NLS-2$
				return Responses.collection(resultRows.build(), null, null, null, collectionName);
			}

            if (returnType != null && returnType.isSimple()) {
            	Object result = stmt.getObject(1);
            	if (result == null) {
            		result = org.odata4j.expression.Expression.null_();
            	}
            	return Responses.simple((EdmSimpleType)returnType, "return", result); //$NON-NLS-1$
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
	}

	@Override
	public MetadataStore getMetadataStore() {
		MetadataStore store = null;
		ConnectionImpl connection = null;
		long currentTime = System.currentTimeMillis();
		if (this.metadataStore == null || this.lastLookup == -1 || currentTime-this.lastLookup > this.cacheTime) {
			try {
				connection = getConnection();
				LocalServerConnection lsc = (LocalServerConnection)connection.getServerConnection();
				VDBMetaData vdb = lsc.getWorkContext().getVDB();
				if (vdb == null) {
					throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16001, this.vdbName, this.vdbVersion));
				}
				store = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
			} catch (SQLException e) {
				throw new ServerErrorException(e.getMessage(),e);
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
						
					}
				}
			}
		}

		// this is check if the vdb has reloaded between calls. Hopefully the above look up is not expensive and by doing
		// object id match we can figure out there has been change in vdb load
		if (this.metadataStore == null || this.metadataStore != store) {
			this.metadataStore = store;
			this.edmMetaData = null;
		}
		return this.metadataStore;
	}


	@Override
	public EntityList executeSQL(Query query, List<SQLParam> parameters, EdmEntitySet entitySet, Map<String, Boolean> projectedColumns, boolean useSkipToken, String skipToken, boolean getCount) {
		Connection connection = null;
		try {
			if (useSkipToken) {
				CacheHint hint = new CacheHint();
				hint.setTtl(this.cacheTime);
				hint.setScope(CacheDirective.Scope.USER);
				query.setCacheHint(hint);
			}

			String sql = query.toString();

			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:",sql); //$NON-NLS-1$

			connection = getConnection();
			final PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			if (parameters!= null && !parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}

			final ResultSet rs = stmt.executeQuery();
            int skipSize = 0;
            if (skipToken != null) {
            	skipSize = Integer.parseInt(skipToken);
            }
			return new EntityList(projectedColumns, entitySet, rs, skipSize, this.batchSize, getCount);
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
			int count = rs.getInt(1);
			rs.close();
			stmt.close();
			return Responses.count(count);
		} catch (Exception e) {
			throw new ServerErrorException(e.getMessage(), e);
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
	public int executeUpdate(Command query, List<SQLParam> parameters) {
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
			int count = stmt.executeUpdate();
			stmt.close();
			return count;
		} catch (Exception e) {
			throw new ServerErrorException(e.getMessage(), e);
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
	public EdmDataServices getMetadata() {
		if (this.edmMetaData == null) {
			this.edmMetaData = ODataEntitySchemaBuilder.buildMetadata(getMetadataStore());
		}
		return this.edmMetaData;
	}
}
