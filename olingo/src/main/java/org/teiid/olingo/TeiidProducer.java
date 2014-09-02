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

import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.ContextURL.Suffix;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.format.ODataFormat;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataTranslatedException;
import org.apache.olingo.server.api.processor.DefaultProcessor;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.teiid.query.sql.lang.Query;

public class TeiidProducer extends DefaultProcessor implements EntityCollectionProcessor, EntityProcessor {
	private Client client;
	private OData odata;
	private Edm edm;

	public TeiidProducer(Client client) {
		this.client = client;
	}

	@Override
	public void init(final OData odata, final Edm edm) {
		super.init(odata, edm);
		this.odata = odata;
		this.edm = edm;
	}
	/*
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
	*/

	@Override
	public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType format) {
		if (!validateOptions(uriInfo.asUriInfoResource())) {
			response.setStatusCode(HttpStatusCode.NOT_IMPLEMENTED.getStatusCode());
			return;
		}
		
		try {
			final EdmEntitySet entitySet = getEdmEntitySet(uriInfo.asUriInfoResource());
			if (entitySet == null) {
				throw new ODataApplicationException(null, 0, null);
			}
			
			ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), true);
			Query query = visitor.selectString(entitySet.getName(), uriInfo);
			List<SQLParam> parameters = visitor.getParameters();
			List<Entity> entityList =  this.client.executeSQL(query, parameters, entitySet, visitor.getProjectedColumns(), uriInfo);

			if (entityList.isEmpty()) {
				response.setStatusCode(HttpStatusCode.NOT_FOUND.getStatusCode());
			}
			
			ODataSerializer serializer = this.odata.createSerializer(ODataFormat.fromContentType(format));
			response.setContent(serializer.entity(entitySet, entityList.get(0), getContextUrl(entitySet, true)));
			response.setStatusCode(HttpStatusCode.OK.getStatusCode());
			response.setHeader(HttpHeader.CONTENT_TYPE,format.toContentTypeString());
			
		} catch (final ODataTranslatedException e) {
			response.setStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
		} catch (final ODataApplicationException e) {
			response.setStatusCode(e.getStatusCode());
		}	
	}
	
/*	@Override
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
	*/
	
	private boolean validateOptions(final UriInfoResource uriInfo) {
		return uriInfo.getCountOption() == null
				&& uriInfo.getCustomQueryOptions().isEmpty()
				&& uriInfo.getExpandOption() == null
				&& uriInfo.getFilterOption() == null
				&& uriInfo.getIdOption() == null
				&& uriInfo.getOrderByOption() == null
				&& uriInfo.getSearchOption() == null
				&& uriInfo.getSelectOption() == null
				&& uriInfo.getSkipOption() == null
				&& uriInfo.getSkipTokenOption() == null
				&& uriInfo.getTopOption() == null;
	}

	private org.apache.olingo.commons.api.edm.EdmEntitySet getEdmEntitySet(final UriInfoResource uriInfo) throws ODataApplicationException {
		final List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
		if (resourcePaths.size() != 1) {
			throw new ODataApplicationException("Invalid resource path.",HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
		}
		if (!(resourcePaths.get(0) instanceof UriResourceEntitySet)) {
			throw new ODataApplicationException("Invalid resource type.",HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
		}
		final UriResourceEntitySet uriResource = (UriResourceEntitySet) resourcePaths.get(0);
		if (uriResource.getTypeFilterOnCollection() != null|| uriResource.getTypeFilterOnEntry() != null) {
			throw new ODataApplicationException("Type filters are not supported.",HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
		}
		return uriResource.getEntitySet();
	}

	private ContextURL getContextUrl(final org.apache.olingo.commons.api.edm.EdmEntitySet entitySet, final boolean isSingleEntity) {
		return ContextURL.Builder.create().entitySet(entitySet).suffix(isSingleEntity ? Suffix.ENTITY : null).build();
	}


	@Override
	public void readCollection(ODataRequest request, ODataResponse response,
			UriInfo uriInfo, ContentType requestedContentType) {
		// rameshTODO Auto-generated method stub
		
	}
}

