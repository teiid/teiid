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
import org.odata4j.exceptions.NotImplementedException;
import org.odata4j.exceptions.ServerErrorException;
import org.odata4j.producer.*;
import org.odata4j.producer.edm.MetadataProducer;
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
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.LocalServerConnection;

public class TeiidProducer implements ODataProducer {
	private EdmDataServices metadata;
	private MetadataStore metadataStore;
	private String vdbName;
	private int vdbVersion;
	
	public TeiidProducer(String vdbName, int version) {
		this.vdbName = vdbName;
		this.vdbVersion = version;
		this.metadataStore = getMetadataStore();
	}
	
	private ConnectionImpl getConnection(String vdbName, int version) throws SQLException {
		TeiidDriver driver = new TeiidDriver();
		return driver.connect("jdbc:teiid:"+vdbName+"."+version+";", null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}		
	
	@Override
	public <TExtension extends OExtension<ODataProducer>> TExtension findExtension(Class<TExtension> clazz) {
		return null;
	}

	@Override
	public EdmDataServices getMetadata() {
		if (this.metadata == null) {
			this.metadata = ODataEntitySchemaBuilder.buildMetadata(metadataStore);
		}
		return this.metadata;
	}

	@Override
	public MetadataProducer getMetadataProducer() {
		return new MetadataProducer(this, null);
	}

	@Override
	public EntitiesResponse getEntities(ODataContext context, String entitySetName, QueryInfo queryInfo) {
		EdmEntitySet entitySet = getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(this.metadataStore, true);
		Query query = visitor.selectString(entitySetName, queryInfo, null, null, false);
		List<SQLParam> parameters = visitor.getParameters();
		entitySet = getEntitySet(visitor.getEntityTable().getFullName());
		EntityList entityList =  sqlExecute(query.toString(), parameters, entitySet, visitor.getProjectedColumns());
		return Responses.entities(entityList, entitySet, null, null);
	}

	private EdmEntitySet getEntitySet(String entitySetName) {
		EdmDataServices eds = getMetadata();
		return eds.getEdmEntitySet(entitySetName);
	}

	@Override
	public CountResponse getEntitiesCount(ODataContext context, String entitySetName, QueryInfo queryInfo) {
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(this.metadataStore, true);
		Query query = visitor.selectString(entitySetName, queryInfo, null, null, false);
		List<SQLParam> parameters = visitor.getParameters();
		return sqlExecuteCount(query.toString(), parameters);
	}
	
	@Override
	public EntityResponse getEntity(ODataContext context, String entitySetName, OEntityKey entityKey, EntityQueryInfo queryInfo) {
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadataStore, true);
		Query query = visitor.selectString(entitySetName, queryInfo, entityKey, null, false);
		EdmEntitySet entitySet = getEntitySet(visitor.getEntityTable().getFullName());
		List<SQLParam> parameters = visitor.getParameters();
		EntityList entityList =  sqlExecute(query.toString(), parameters, entitySet, visitor.getProjectedColumns());
		if (entityList.isEmpty()) {
			return null;
		}
		return Responses.entity(entityList.get(0));
	}

	@Override
	public BaseResponse getNavProperty(ODataContext context,
			String entitySetName, OEntityKey entityKey, String navProp,
			QueryInfo queryInfo) {
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadataStore, true);
		Query query = visitor.selectString(entitySetName, queryInfo, entityKey, navProp, false);
		List<SQLParam> parameters = visitor.getParameters();
		EdmEntitySet entitySet = getEntitySet(visitor.getEntityTable().getFullName());
		EntityList entityList =  sqlExecute(query.toString(), parameters, entitySet, visitor.getProjectedColumns());
		return Responses.entities(entityList, entitySet, null, null);
	}

	@Override
	public CountResponse getNavPropertyCount(ODataContext context,
			String entitySetName, OEntityKey entityKey, String navProp,
			QueryInfo queryInfo) {
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadataStore, true);
		Query query = visitor.selectString(entitySetName, queryInfo, entityKey, navProp, false);
		List<SQLParam> parameters = visitor.getParameters();
		return sqlExecuteCount(query.toString(), parameters);
	}

	@Override
	public void close() {
	}

	@Override
	public EntityResponse createEntity(ODataContext context, String entitySetName, OEntity entity) {
		EdmEntitySet entitySet = getEntitySet(entitySetName);
		
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadataStore, true);
		Insert query = visitor.insert(entitySet, entity);
				
		List<SQLParam> parameters = visitor.getParameters();
		int updateCount =  sqlExecuteUpdate(query.toString(), parameters);
		if (updateCount  == 1) {
			visitor = new ODataSQLBuilder(metadataStore, true);
		    OEntityKey entityKey = visitor.buildEntityKey(entitySet, entity);
		    LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_ODATA, null, "created entity = ", entitySetName, " with key=", entityKey.asSingleValue()); //$NON-NLS-1$
		    return getEntity(context, entitySetName, entityKey, EntityQueryInfo.newBuilder().build());
		}
		return null;
	}

	@Override
	public EntityResponse createEntity(ODataContext context, String entitySetName, OEntityKey entityKey, String navProp, OEntity entity) {
		// deep inserts are not currently supported.
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteEntity(ODataContext context, String entitySetName, OEntityKey entityKey) {
		EdmEntitySet entitySet = getEntitySet(entitySetName);
		
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadataStore, true);
		Delete query = visitor.delete(entitySet, entityKey);
				
		List<SQLParam> parameters = visitor.getParameters();
		int deleteCount =  sqlExecuteUpdate(query.toString(), parameters);
		if (deleteCount > 0) {
			LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_ODATA, null, "deleted entity = ", entitySetName, " with key=", entityKey.asSingleValue()); //$NON-NLS-1$
		}
	}

	@Override
	public void mergeEntity(ODataContext context, String entitySetName, OEntity entity) {
		int updateCount = update(entitySetName, entity);
		if (updateCount == 0) {
			createEntity(context, entitySetName, entity);
		}
	}

	@Override
	public void updateEntity(ODataContext context, String entitySetName, OEntity entity) {
		int updateCount = update(entitySetName, entity);
		if (updateCount > 0) {
			LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_ODATA, null, "updated entity = ", entitySetName, " with key=", entity.getEntityKey().asSingleValue()); //$NON-NLS-1$
		}		
	}

	private int update(String entitySetName, OEntity entity) {
		EdmEntitySet entitySet = getEntitySet(entitySetName);
		
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadataStore, true);
		Update query = visitor.update(entitySet, entity);
				
		List<SQLParam> parameters = visitor.getParameters();
		return sqlExecuteUpdate(query.toString(), parameters);
	}

	@Override
	public EntityIdResponse getLinks(ODataContext context,
			OEntityId sourceEntity, String targetNavProp) {
		BaseResponse response = getNavProperty(context,sourceEntity.getEntitySetName(), sourceEntity.getEntityKey(),targetNavProp, new QueryInfo());
		if (response instanceof EntitiesResponse) {
			EntitiesResponse er = (EntitiesResponse) response;
			return Responses.multipleIds(er.getEntities());
		}
		if (response instanceof EntityResponse) {
			EntityResponse er = (EntityResponse) response;
			return Responses.singleId(er.getEntity());
		}
		throw new NotImplementedException(sourceEntity + " " + targetNavProp);
	}

	@Override
	public void createLink(ODataContext context, OEntityId sourceEntity, String targetNavProp, OEntityId targetEntity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateLink(ODataContext context, OEntityId sourceEntity,
			String targetNavProp, OEntityKey oldTargetEntityKey,
			OEntityId newTargetEntity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteLink(ODataContext context, OEntityId sourceEntity, String targetNavProp, OEntityKey targetEntityKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public BaseResponse callFunction(ODataContext context,
			EdmFunctionImport function, Map<String, OFunctionParameter> params,
			QueryInfo queryInfo) {
		
		EdmEntityContainer eec = findEntityContainer(function);
		StringBuilder sql = new StringBuilder();
		// fully qualify the procedure name
		if (function.getReturnType() != null && function.getReturnType().isSimple()) {
			sql.append("{? = ");
		}
		else {
			sql.append("{");
		}
		sql.append("call ").append(eec.getName()+"."+function.getName());
		sql.append("(");
		if (!params.isEmpty()) {
			for (int i = 0; i < params.size()-1; i++) {
				sql.append("?, ");
			}
			sql.append("?");
		}
		sql.append(")");
		sql.append("}");
		return sqlExecuteCall(sql.toString(), params, function.getReturnType());
	}
	
	private BaseResponse sqlExecuteCall(String sql, Map<String, OFunctionParameter> parameters, EdmType returnType) {
		ConnectionImpl connection = null;
		try {
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

	private MetadataStore getMetadataStore() {
		try {
			InitialContext ic = new InitialContext();
			ClientServiceRegistry csr = (ClientServiceRegistry)ic.lookup(LocalServerConnection.TEIID_RUNTIME_CONTEXT);
			VDBRepository repo = csr.getClientService(VDBRepository.class);
			VDBMetaData vdb = repo.getVDB(this.vdbName, this.vdbVersion);
			if (vdb == null) {
				throw new NotFoundException("VDB="+this.vdbName+" with version="+this.vdbVersion+" not found");
			}
			return vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		} catch (NamingException e) {
			 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40067, e);
		} catch (ComponentNotFoundException e) {
			throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40067, e);
		}
	}
	
	private EntityList sqlExecute(String sql, List<SQLParam> parameters, EdmEntitySet entitySet, HashMap<String, Boolean> projectedColumns) {
		try {
			LogManager.logInfo(LogConstants.CTX_ODATA, "Teiid-Query:"+sql); //$NON-NLS-1$
			
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
	
	private CountResponse sqlExecuteCount(String sql, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
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
	
	private int sqlExecuteUpdate(String sql, List<SQLParam> parameters) {
		ConnectionImpl connection = null;
		try {
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
	
	EdmEntityContainer findEntityContainer(EdmFunctionImport function) {
		EdmDataServices eds = getMetadata();
		for (EdmSchema schema : eds.getSchemas()) {
			for (EdmEntityContainer eec:schema.getEntityContainers()) {
				for (EdmFunctionImport func:eec.getFunctionImports()) {
					if (func == function) {
						return eec;
					}
				}
			}
		}
		return null;		
	}
}
	
