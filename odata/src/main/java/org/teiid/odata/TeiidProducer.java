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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityId;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OExtension;
import org.odata4j.core.OFunctionParameter;
import org.odata4j.core.OSimpleObject;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmFunctionParameter;
import org.odata4j.edm.EdmSchema;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.exceptions.NotImplementedException;
import org.odata4j.producer.*;
import org.odata4j.producer.edm.MetadataProducer;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.translator.odata.ODataTypeManager;

public class TeiidProducer implements ODataProducer {
	private Client client;

	public TeiidProducer(Client client) {
		this.client = client;
	}

	@Override
	public <TExtension extends OExtension<ODataProducer>> TExtension findExtension(Class<TExtension> clazz) {
		return null;
	}

	@Override
	public EdmDataServices getMetadata() {
		return this.client.getMetadata();
	}

	@Override
	public MetadataProducer getMetadataProducer() {
		return new MetadataProducer(this, null);
	}

	@Override
	public EntitiesResponse getEntities(ODataContext context, String entitySetName, QueryInfo queryInfo) {
		return getNavProperty(context, entitySetName, null, null, queryInfo);
	}

	@Override
	public EntitiesResponse getNavProperty(ODataContext context, String entitySetName, OEntityKey entityKey, String navProp, final QueryInfo queryInfo) {
		checkExpand(queryInfo);
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		Query query = visitor.selectString(entitySetName, queryInfo, entityKey, navProp, false);
		final EdmEntitySet entitySet = getEntitySet(visitor.getEntityTable().getFullName());
		List<SQLParam> parameters = visitor.getParameters();
		final EntityList entities = this.client.executeSQL(query, parameters, entitySet, visitor.getProjectedColumns(), queryInfo);
		return new EntitiesResponse() {
			@Override
			public List<OEntity> getEntities() {
				return entities;
			}
			@Override
			public EdmEntitySet getEntitySet() {
				return entitySet;
			}
			@Override
			public Integer getInlineCount() {
				if (queryInfo.inlineCount == InlineCount.ALLPAGES) {
					return entities.getCount();
				}
				return null;
			}
			@Override
			public String getSkipToken() {
				return entities.nextToken();
			}
		};
	}

	private void checkExpand(QueryInfo queryInfo) {
		if (queryInfo != null && queryInfo.expand != null && !queryInfo.expand.isEmpty()) {
			throw new UnsupportedOperationException("Expand is not supported"); //$NON-NLS-1$
		}
	}

	private EdmEntitySet getEntitySet(String entitySetName) {
		EdmDataServices eds = getMetadata();
		EdmEntitySet entity =  eds.findEdmEntitySet(entitySetName);
		if (entity == null) {
			throw new NotFoundException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16011, entitySetName));
		}
		return entity;
	}

	@Override
	public CountResponse getEntitiesCount(ODataContext context, String entitySetName, QueryInfo queryInfo) {
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		Query query = visitor.selectString(entitySetName, queryInfo, null, null, true);
		List<SQLParam> parameters = visitor.getParameters();
		return this.client.executeCount(query, parameters);
	}

	@Override
	public EntityResponse getEntity(ODataContext context, String entitySetName, OEntityKey entityKey, EntityQueryInfo queryInfo) {
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		Query query = visitor.selectString(entitySetName, queryInfo, entityKey, null, false);
		EdmEntitySet entitySet = getEntitySet(visitor.getEntityTable().getFullName());
		List<SQLParam> parameters = visitor.getParameters();
		List<OEntity> entityList =  this.client.executeSQL(query, parameters, entitySet, visitor.getProjectedColumns(), null);
		if (entityList.isEmpty()) {
			return null;
		}
		return Responses.entity(entityList.get(0));
	}

	@Override
	public CountResponse getNavPropertyCount(ODataContext context,
			String entitySetName, OEntityKey entityKey, String navProp,
			QueryInfo queryInfo) {
		getEntitySet(entitySetName); // validate entity
		ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		Query query = visitor.selectString(entitySetName, queryInfo, entityKey, navProp, true);
		List<SQLParam> parameters = visitor.getParameters();
		return this.client.executeCount(query, parameters);
	}

	@Override
	public void close() {
	}

	@Override
	public EntityResponse createEntity(ODataContext context, String entitySetName, OEntity entity) {
		EdmEntitySet entitySet = getEntitySet(entitySetName);

		ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		Insert query = visitor.insert(entitySet, entity);

		List<SQLParam> parameters = visitor.getParameters();
		UpdateResponse response =  this.client.executeUpdate(query, parameters);
		if (response.getUpdateCount()  == 1) {
			visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		    OEntityKey entityKey = visitor.buildEntityKey(entitySet, entity, response.getGeneratedKeys());
		    LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_ODATA, null, "created entity = ", entitySetName, " with key=", entityKey.toString()); //$NON-NLS-1$ //$NON-NLS-2$
		    return getEntity(context, entitySetName, entityKey, EntityQueryInfo.newBuilder().build());
		}
		return null;
	}

	@Override
	public EntityResponse createEntity(ODataContext context, String entitySetName, OEntityKey entityKey, String navProp, OEntity entity) {
		// deep inserts are not currently supported.
		throw new UnsupportedOperationException("Deep inserts are not supported"); //$NON-NLS-1$
	}

	@Override
	public void deleteEntity(ODataContext context, String entitySetName, OEntityKey entityKey) {
		EdmEntitySet entitySet = getEntitySet(entitySetName);

		ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		Delete query = visitor.delete(entitySet, entityKey);

		List<SQLParam> parameters = visitor.getParameters();
		UpdateResponse response =  this.client.executeUpdate(query, parameters);
		if (response.getUpdateCount() == 0) {
			LogManager.log(MessageLevel.INFO, LogConstants.CTX_ODATA, null, "no entity to delete in = ", entitySetName, " with key=", entityKey.toString()); //$NON-NLS-1$ //$NON-NLS-2$
		} else if (response.getUpdateCount() == 1) {
			LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_ODATA, null, "deleted entity = ", entitySetName, " with key=", entityKey.toString()); //$NON-NLS-1$ //$NON-NLS-2$
		} else {
			//TODO: should this be an exception (a little too late, may need validation that we are dealing with a single entity first)
			LogManager.log(MessageLevel.WARNING, LogConstants.CTX_ODATA, null, "deleted multiple entities = ", entitySetName, " with key=", entityKey.toString()); //$NON-NLS-1$ //$NON-NLS-2$
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
			LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_ODATA, null, "updated entity = ", entitySetName, " with key=", entity.getEntityKey().toString()); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}

	private int update(String entitySetName, OEntity entity) {
		EdmEntitySet entitySet = getEntitySet(entitySetName);

		ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
		Update query = visitor.update(entitySet, entity);

		List<SQLParam> parameters = visitor.getParameters();
		UpdateResponse response = this.client.executeUpdate(query, parameters);
		return response.getUpdateCount();
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
		throw new NotImplementedException(sourceEntity + " " + targetNavProp); //$NON-NLS-1$
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
		checkExpand(queryInfo);
		EdmEntityContainer eec = findEntityContainer(function);
		StringBuilder sql = new StringBuilder();
		// fully qualify the procedure name
		if (function.getReturnType() != null && function.getReturnType().isSimple()) {
			sql.append("{? = "); //$NON-NLS-1$
		}
		else {
			sql.append("{"); //$NON-NLS-1$
		}
		sql.append("call ").append(eec.getName()+"."+SQLStringVisitor.escapeSinglePart(function.getName())); //$NON-NLS-1$ //$NON-NLS-2$
		sql.append("("); //$NON-NLS-1$
		List<SQLParam> sqlParams = new ArrayList<SQLParam>();
		if (!params.isEmpty()) {
			List<EdmFunctionParameter> metadataParams = function.getParameters();
			boolean first = true;
			for (EdmFunctionParameter edmFunctionParameter : metadataParams) {
				OFunctionParameter param = params.get(edmFunctionParameter.getName());
				if (param == null) {
					continue;
				}
				if (!first) {
					sql.append(","); //$NON-NLS-1$
				}
				sql.append(SQLStringVisitor.escapeSinglePart(edmFunctionParameter.getName())).append("=>?"); //$NON-NLS-1$
				first = false;
				Object value = ((OSimpleObject<?>)(param.getValue())).getValue();
				Integer sqlType = JDBCSQLTypeInfo.getSQLType(ODataTypeManager.teiidType(param.getType().getFullyQualifiedTypeName()));
				sqlParams.add(new SQLParam(ODataTypeManager.convertToTeiidRuntimeType(value), sqlType));
			}
		}
		sql.append(")"); //$NON-NLS-1$
		sql.append("}"); //$NON-NLS-1$
		return this.client.executeCall(sql.toString(), sqlParams, function.getReturnType());
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

