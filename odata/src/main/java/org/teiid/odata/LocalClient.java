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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.odata4j.core.OCollection;
import org.odata4j.core.OCollections;
import org.odata4j.core.OComplexObject;
import org.odata4j.core.OComplexObjects;
import org.odata4j.core.OFunctionParameter;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.core.OSimpleObject;
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
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.deployers.VDBRepository;
import org.teiid.jdbc.CallableStatementImpl;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.EmbeddedProfile;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.odata.ODataTypeManager;
import org.teiid.transport.ClientServiceRegistry;
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
	
	public LocalClient(String vdbName, int vdbVersion, Properties props) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.batchSize = PropertiesUtils.getIntProperty(props, BATCH_SIZE, 256);
		this.cacheTime = PropertiesUtils.getLongProperty(props, SKIPTOKEN_TIME, 300000L);
		this.transportName = props.getProperty(EmbeddedProfile.TRANSPORT_NAME, "odata"); //$NON-NLS-1$
	}
	
	@Override
	public String getVDBName() {
		return this.vdbName;
	}

	@Override
	public int getVDBVersion() {
		return this.vdbVersion;
	}
	
	
	private ConnectionImpl getConnection() throws SQLException {
		TeiidDriver driver = new TeiidDriver();
		StringBuilder sb = new StringBuilder();
		sb.append("jdbc:teiid:").append(this.vdbName).append(".").append(this.vdbVersion).append(";");
		sb.append("PassthroughAuthentication=true;");
		sb.append("useCallingThread=true;");
		sb.append("resultSetCacheMode=true;");
		sb.append(EmbeddedProfile.TRANSPORT_NAME).append("=").append(this.transportName).append(";");
		return driver.connect(sb.toString(), null);
	}	
	
	@Override
	public BaseResponse executeCall(String sql, Map<String, OFunctionParameter> parameters, EdmType returnType) {
		ConnectionImpl connection = null;
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection();
			final CallableStatementImpl stmt = connection.prepareCall(sql);
			
			int i = 1;
			if (returnType != null && returnType.isSimple()) {
				stmt.registerOutParameter(i++, JDBCSQLTypeInfo.getSQLType(ODataTypeManager.teiidType(returnType.getFullyQualifiedTypeName())));
			}
			
			if (!parameters.isEmpty()) {
				for (String key:parameters.keySet()) {
					OFunctionParameter param = parameters.get(key);
					stmt.setObject(i++, ((OSimpleObject)param.getValue()).getValue());
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
                collectionName = collectionName.replace("(", "_");
                collectionName = collectionName.replace(")", "_");
				return Responses.collection(resultRows.build(), null, null, null, collectionName);				
			}
			
            if (returnType != null && returnType.isSimple()) {
            	Object result = stmt.getObject(1);
            	if (result == null) {
            		result = org.odata4j.expression.Expression.null_();
            	}
            	return Responses.simple((EdmSimpleType)returnType, "return", result);
            }			
			return Responses.simple(EdmSimpleType.INT32, 1);
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
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
		if (this.metadataStore == null) {
			try {
				InitialContext ic = new InitialContext();
				ClientServiceRegistry csr = (ClientServiceRegistry)ic.lookup(LocalServerConnection.jndiNameForRuntime(this.transportName));
				VDBRepository repo = csr.getClientService(VDBRepository.class);
				VDBMetaData vdb = repo.getVDB(this.vdbName, this.vdbVersion);
				if (vdb == null) {
					throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16001, this.vdbName, this.vdbVersion));
				}
				this.metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
			} catch (NamingException e) {
				 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40067, e);
			} catch (ComponentNotFoundException e) {
				throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40067, e);
			}
		}
		return this.metadataStore;
	}

	
	@Override
	public EntityList executeSQL(Query query, List<SQLParam> parameters, EdmEntitySet entitySet, Map<String, Boolean> projectedColumns, boolean useSkipToken, String skipToken) {
		try {
			if (useSkipToken) {
				CacheHint hint = new CacheHint();
				hint.setTtl(this.cacheTime);
				hint.setScope(CacheDirective.Scope.USER);
				query.setCacheHint(hint);
			}
			
			String sql = query.toString();
					
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			
			final ConnectionImpl connection = getConnection();
			final PreparedStatementImpl stmt = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			if (parameters!= null && !parameters.isEmpty()) {
				for (int i = 0; i < parameters.size(); i++) {
					stmt.setObject(i+1, parameters.get(i).value, parameters.get(i).sqlType);
				}
			}
			
			ResultsFuture<Boolean> executionFuture = stmt.submitExecute(ResultsMode.RESULTSET, null);
			if (executionFuture.get()) {
				final ResultSet rs = stmt.getResultSet();
				ResultsFuture<Boolean> result = new ResultsFuture<Boolean>();
                result.addCompletionListener(new ResultsFuture.CompletionListener<Boolean>() {
                	public void onCompletion(ResultsFuture<Boolean> future) {
						try {
	                		try {
								stmt.close();
								connection.close();
							} catch (SQLException e) {
								LogManager.logDetail(LogConstants.CTX_ODATA, e, "Error closing statement"); //$NON-NLS-1$
							}
							future.get();
						} catch (Throwable e) {
							//ignore
						}
                	}
                });				
                int skipSize = 0;
                if (skipToken != null) {
                	skipSize = Integer.parseInt(skipToken);
                }
				return new EntityList(projectedColumns, entitySet, rs, result, skipSize, this.batchSize);
			}
			return null;
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		}
	}
	
	@Override
	public CountResponse executeCount(Query query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
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
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
	}	
	
	@Override
	public int executeUpdate(Command query, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			String sql = query.toString();
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
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
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
	}

	@Override
	public EdmDataServices getMetadata() {
		return ODataEntitySchemaBuilder.buildMetadata(getMetadataStore());
	}
}
