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
import java.util.*;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.odata4j.core.*;
import org.odata4j.edm.*;
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
import org.teiid.deployers.VDBRepository;
import org.teiid.jdbc.CallableStatementImpl;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.PreparedStatementImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.LocalServerConnection;

public class LocalClient implements Client {
	private MetadataStore metadataStore;
	private String vdbName;
	private int vdbVersion;
	
	public LocalClient(String vdbName, int vdbVersion) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
	}
	
	@Override
	public String getVDBName() {
		return this.vdbName;
	}

	@Override
	public int getVDBVersion() {
		return this.vdbVersion;
	}
	
	private ConnectionImpl getConnection(String vdbName, int version) throws SQLException {
		TeiidDriver driver = new TeiidDriver();
		return driver.connect("jdbc:teiid:"+vdbName+"."+version+";", null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}	

	@Override
	public BaseResponse sqlExecuteCall(String sql, Map<String, OFunctionParameter> parameters, EdmType returnType) {
		ConnectionImpl connection = null;
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection(this.vdbName, this.vdbVersion);
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
				ClientServiceRegistry csr = (ClientServiceRegistry)ic.lookup(LocalServerConnection.TEIID_RUNTIME_CONTEXT);
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
	public List<OEntity> sqlExecute(String sql, List<SQLParam> parameters, EdmEntitySet entitySet, Map<String, Boolean> projectedColumns) {
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			
			final ConnectionImpl connection = getConnection(this.vdbName, this.vdbVersion);
			final PreparedStatementImpl stmt = connection.prepareCall(sql);
			if (!parameters.isEmpty()) {
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
				return new EntityList(projectedColumns, entitySet, rs, result);
			}
			return null;
		} catch (SQLException e) {
			throw new ServerErrorException(e.getMessage(), e);
		} catch (Throwable e) {
			throw new ServerErrorException(e.getMessage(), e);
		}
	}
	
	@Override
	public CountResponse sqlExecuteCount(String sql, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection(this.vdbName, this.vdbVersion);
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
	public int sqlExecuteUpdate(String sql, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			connection = getConnection(this.vdbName, this.vdbVersion);
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
