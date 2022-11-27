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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmParameter;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.core.ServiceHandler;
import org.apache.olingo.server.core.ServiceRequest;
import org.apache.olingo.server.core.requests.ActionRequest;
import org.apache.olingo.server.core.requests.DataRequest;
import org.apache.olingo.server.core.requests.FunctionRequest;
import org.apache.olingo.server.core.requests.MediaRequest;
import org.apache.olingo.server.core.requests.MetadataRequest;
import org.apache.olingo.server.core.requests.OperationRequest;
import org.apache.olingo.server.core.requests.ServiceDocumentRequest;
import org.apache.olingo.server.core.responses.*;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.odata.api.BaseResponse;
import org.teiid.odata.api.Client;
import org.teiid.odata.api.ComplexResponse;
import org.teiid.odata.api.QueryResponse;
import org.teiid.odata.api.UpdateResponse;
import org.teiid.olingo.EdmComplexResponse;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.service.ProcedureSQLBuilder.ActionParameterValueProvider;
import org.teiid.olingo.service.ProcedureSQLBuilder.FunctionParameterValueProvider;
import org.teiid.olingo.service.ProcedureSQLBuilder.ProcedureReturn;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.XMLSerialize;

public class TeiidServiceHandler implements ServiceHandler {

    static class ExpandNode {
        EdmNavigationProperty navigationProperty;
        List<ExpandNode> children = new ArrayList<TeiidServiceHandler.ExpandNode>();
    }

    private static final String PREFERENCE_APPLIED = "Preference-Applied";
    private static final String ODATA_MAXPAGESIZE = "odata.maxpagesize";
    private boolean prepared = true;
    private OData odata;
    private ServiceMetadata serviceMetadata;
    private Integer maxPageSize = null;

    private static ThreadLocal<Client> CLIENT = new ThreadLocal<Client>();

    public static Client getClient() {
        return CLIENT.get();
    }

    public static void setClient(Client client) {
        CLIENT.set(client);
    }

    public TeiidServiceHandler(String schemaName) {
    }

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    public void setPrepared(boolean flag) {
        this.prepared = flag;
    }

    @Override
    public void readMetadata(MetadataRequest request, MetadataResponse response)
            throws ODataLibraryException, ODataApplicationException {
        response.writeMetadata();
    }

    @Override
    public void readServiceDocument(ServiceDocumentRequest request,
            ServiceDocumentResponse response) throws ODataLibraryException,
            ODataApplicationException {
        response.writeServiceDocument(request.getODataRequest().getRawBaseUri());
    }

    static class UniqueNameGenerator {
        private int groupCount;
        public String getNextGroup() {
            String aliasGroup = "g"+this.groupCount++; //$NON-NLS-1$
            return aliasGroup;
        }
    }

    @Override
    public <T extends ServiceResponse> void read(final DataRequest request, T response)
            throws ODataLibraryException, ODataApplicationException {

        final ODataSQLBuilder visitor = new ODataSQLBuilder(odata,
                getClient().getMetadataStore(), this.prepared, true,
                request.getODataRequest().getRawBaseUri(), this.serviceMetadata);
        visitor.visit(request.getUriInfo());

        final BaseResponse queryResponse;
        try {
            Query query = visitor.selectQuery();
            queryResponse = executeQuery(request, request.isCountRequest(), visitor, query);
        } catch (ODataApplicationException|ODataLibraryException e) {
            throw e;
        } catch (Throwable e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }

        response.accepts(new ServiceResponseVisior() {
            public void visit(CountResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                org.teiid.odata.api.CountResponse cr = (org.teiid.odata.api.CountResponse) queryResponse;
                response.writeCount(cr.getCount());
            }

            public void visit(PrimitiveValueResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                EntityCollection entitySet = (EntityCollection)queryResponse;
                if (!entitySet.getEntities().isEmpty()) {
                    Entity entity = entitySet.getEntities().get(0);

                    EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
                    Property property = entity.getProperty(edmProperty.getName());
                    if (property == null) {
                        response.writeNotFound(true);
                    }
                    else if (property.getValue() == null) {
                        response.writeNoContent(true);
                    } else {
                        response.write(property.getValue());
                    }
                } else {
                    response.writeNotFound(true);
                }
            }

            public void visit(PropertyResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                EntityCollection entitySet = (EntityCollection)queryResponse;
                if (!entitySet.getEntities().isEmpty()) {
                    Entity entity = entitySet.getEntities().get(0);

                    EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
                    Property property = entity.getProperty(edmProperty.getName());
                    response.writeProperty(edmProperty.getType(), property);
                }
                else {
                    response.writeNotFound(true);
                }
            }

            public void visit(StreamResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                EntityCollectionResponse entitySet = (EntityCollectionResponse)queryResponse;

                EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
                Object value = entitySet.getStream(edmProperty.getName());
                if (value == null) {
                    response.writeNoContent(true);
                }
                else {
                    try {
                        handleLobResult(getClient().getProperty(Client.CHARSET), value, response);
                    } catch (SQLException e) {
                        LogManager.logDetail(LogConstants.CTX_ODATA, e);
                        response.writeServerError(true);
                    }
                }
            }

            public void visit(EntityResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                EntityCollection entitySet = (EntityCollection)queryResponse;
                if (entitySet.getEntities().isEmpty()) {
                    if (visitor.hasNavigation()) {
                        response.writeNoContent(true);
                    } else {
                        response.writeNotFound(true);
                    }
                } else {
                    response.writeReadEntity((EdmEntityType)visitor.getContext().getEdmStructuredType(),
                        entitySet.getEntities().get(0));
                }
            }

            public void visit(EntitySetResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                sendResults(request, visitor, queryResponse, response);
            }
        });
    }

    private void sendResults(final DataRequest request,
            final ODataSQLBuilder visitor,
            final BaseResponse queryResponse, EntitySetResponse response)
            throws ODataApplicationException, SerializerException {
        if (request.getPreference(ODATA_MAXPAGESIZE) != null) {
            response.writeHeader(PREFERENCE_APPLIED,
                    ODATA_MAXPAGESIZE+"="+ request.getPreference(ODATA_MAXPAGESIZE)); //$NON-NLS-1$
        }
        EntityCollectionResponse result = (EntityCollectionResponse)queryResponse;
        if (result.getNextToken() != null) {
            try {
                String nextUri = request.getODataRequest().getRawBaseUri()
                        +request.getODataRequest().getRawODataPath()
                        + "?"
                        +buildNextToken(request.getODataRequest().getRawQueryPath(), result.getNextToken());
                result.setNext(new URI(nextUri));
            } catch (URISyntaxException e) {
                throw new ODataApplicationException(e.getMessage(), 500, Locale.getDefault(), e);
            }
        }
        response.writeReadEntitySet((EdmEntityType)visitor.getContext().getEdmStructuredType(), result);
    }

    String buildNextToken(final String queryPath, String nextToken) {
        StringBuilder sb = new StringBuilder();
        if (queryPath != null) {
            String[] pairs = queryPath.split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                String key = pair.substring(0, idx);
                if (!key.equals("$skiptoken")) {
                    if (sb.length()>0) {
                        sb.append("&");
                    }
                    sb.append(pair);
                }
            }
        }
        if (sb.length() > 0) {
            sb.append("&$skiptoken=").append(nextToken);
        } else {
            sb.append("$skiptoken=").append(nextToken);
        }
        return sb.toString();
    }

    private void sendResults(final ServiceRequest request,
            final ComplexResponse result, EdmComplexResponse response)
            throws ODataApplicationException, SerializerException {
        if (request.getPreference(ODATA_MAXPAGESIZE) != null) {
            response.writeHeader(PREFERENCE_APPLIED,
                    ODATA_MAXPAGESIZE+"="+ request.getPreference(ODATA_MAXPAGESIZE)); //$NON-NLS-1$
        }
        URI next = null;
        if (result.getNextToken() != null) {
            try {
                String nextUri = request.getODataRequest().getRawBaseUri()
                        +request.getODataRequest().getRawODataPath()
                        + "?"
                        +buildNextToken(request.getODataRequest().getRawQueryPath(), result.getNextToken());
                next = new URI(nextUri);
            } catch (URISyntaxException e) {
                throw new ODataApplicationException(e.getMessage(), 500, Locale.getDefault(), e);
            }
        }
        response.writeComplexType(result, next);
    }

    private BaseResponse executeQuery(final ServiceRequest request, boolean countRequest,
            final ODataSQLBuilder visitor, Query query) throws SQLException {
        if (countRequest) {
            return getClient().executeCount(query, visitor.getParameters());
        }
        int pageSize = getPageSize(request);

        QueryResponse result = null;

        if (visitor.getContext() instanceof CrossJoinNode) {
            result = new CrossJoinResult(request.getODataRequest().getRawBaseUri(),
                    (CrossJoinNode) visitor.getContext());
        } else if (visitor.getContext() instanceof ComplexDocumentNode) {
            ComplexDocumentNode cdn = (ComplexDocumentNode)visitor.getContext();
            result = new OperationResponseImpl(cdn.getProcedureReturn());
        } else if (visitor.getContext() instanceof ApplyDocumentNode) {
            ApplyDocumentNode adn = (ApplyDocumentNode)visitor.getContext();
            result = new ApplyResult(request.getODataRequest().getRawBaseUri(),
                adn);
        } else {
            result = new EntityCollectionResponse(request
                .getODataRequest().getRawBaseUri(),
                visitor.getContext());
        }

        getClient().executeSQL(query, visitor.getParameters(),
                visitor.includeTotalSize(), visitor.getSkip(),
                visitor.getTop(), visitor.getNextToken(), pageSize, result);

        return result;
    }

    private int getPageSize(final ServiceRequest request) {
        if (maxPageSize == null) {
            String pageSize = getClient().getProperty(Client.BATCH_SIZE);
            if (pageSize == null) {
                maxPageSize = BufferManagerImpl.DEFAULT_PROCESSOR_BATCH_SIZE;
            } else {
                maxPageSize = Integer.parseInt(pageSize);
            }
        }
        String pageSize = request.getPreference(ODATA_MAXPAGESIZE);
        if (pageSize == null) {
            return maxPageSize;
        }
        return Math.min(maxPageSize<<4, Integer.parseInt(pageSize));
    }

    private void checkExpand(UriInfoResource queryInfo) {
        if (queryInfo.getExpandOption() != null && !queryInfo.getExpandOption().getExpandItems().isEmpty()) {
            throw new UnsupportedOperationException("Expand is not supported"); //$NON-NLS-1$
        }
    }

    private UpdateResponse performInsert(String rawURI, UriInfo uriInfo,
            EdmEntityType entityType, Entity entity) throws SQLException, TeiidException {
        ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                getClient().getMetadataStore(), this.prepared, false,
                rawURI, this.serviceMetadata);
        visitor.visit(uriInfo);
        Insert command = visitor.insert(entityType, entity, null, this.prepared);
        return getClient().executeUpdate(command, visitor.getParameters());
    }

    private int insertDepth(EdmEntityType entityType, Entity entity) throws SQLException, TeiidException {
        int depth = 1;
        int childDepth = 0;
        for (String navigationName:entityType.getNavigationPropertyNames()) {
            EdmNavigationProperty navProperty = entityType.getNavigationProperty(navigationName);
            Link navLink = entity.getNavigationLink(navigationName);
            if (navLink != null && navLink.getInlineEntity() != null) {
                childDepth = Math.max(childDepth, insertDepth(navProperty.getType(), navLink.getInlineEntity()));
            } else if (navLink != null && navLink.getInlineEntitySet() != null && !navLink.getInlineEntitySet().getEntities().isEmpty()) {
                for (Entity inlineEntity:navLink.getInlineEntitySet().getEntities()) {
                    childDepth = Math.max(childDepth, insertDepth(navProperty.getType(), inlineEntity));
                }
            }
        }
        return depth + childDepth;
    }

    private UpdateResponse performDeepInsert(String rawURI, UriInfo uriInfo,
            EdmEntityType entityType, Entity entity, List<ExpandNode> expandNodes) throws SQLException, TeiidException {
        UpdateResponse response = performInsert(rawURI, uriInfo, entityType, entity);
        for (String navigationName:entityType.getNavigationPropertyNames()) {
            EdmNavigationProperty navProperty = entityType.getNavigationProperty(navigationName);
            Link navLink = entity.getNavigationLink(navigationName);
            if (navLink != null && navLink.getInlineEntity() != null) {
                ExpandNode node = new ExpandNode();
                node.navigationProperty = navProperty;
                expandNodes.add(node);
                performDeepInsert(rawURI, uriInfo, navProperty.getType(), navLink.getInlineEntity(), node.children);
            } else if (navLink != null && navLink.getInlineEntitySet() != null && !navLink.getInlineEntitySet().getEntities().isEmpty()) {
                ExpandNode node = new ExpandNode();
                node.navigationProperty = navProperty;
                expandNodes.add(node);
                for (Entity inlineEntity:navLink.getInlineEntitySet().getEntities()) {
                    performDeepInsert(rawURI, uriInfo, navProperty.getType(), inlineEntity, node.children);
                }
            }
        }
        return response;
    }

    @Override
    public void createEntity(DataRequest request, Entity entity,
            EntityResponse response) throws ODataLibraryException,
            ODataApplicationException {

        EdmEntitySet edmEntitySet = request.getEntitySet();

        if (!request.getNavigations().isEmpty()) {
            //if this is a create to a navigation, it's a different entity type
            UriResourceNavigation lastNavigation = request.getNavigations().getLast();
            edmEntitySet = (EdmEntitySet)edmEntitySet.getRelatedBindingTarget(lastNavigation.getProperty().getName());
        }

        EdmEntityType entityType = edmEntitySet.getEntityType();

        String txn;
        try {
            txn = getClient().startTransaction();
        } catch (SQLException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }
        boolean success = false;

        try {
            List<ExpandNode> expands = new ArrayList<TeiidServiceHandler.ExpandNode>();
            int insertDepth = insertDepth(entityType, entity);
            ODataSQLBuilder.checkExpandLevel(insertDepth - 1); //don't count the root
            UpdateResponse updateResponse = performDeepInsert(request
                    .getODataRequest().getRawBaseUri(), request.getUriInfo(),
                    entityType, entity, expands);

            if (updateResponse != null && updateResponse.getUpdateCount()  == 1) {
                ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                        getClient().getMetadataStore(), true, false,
                        request.getODataRequest().getRawBaseUri(), this.serviceMetadata);

                Query query = visitor.selectWithEntityKey(entityType,
                                entity, updateResponse.getGeneratedKeys(), expands);
                LogManager.logDetail(LogConstants.CTX_ODATA, null, "created entity = ", entityType.getName(), " with key=", query.getCriteria().toString()); //$NON-NLS-1$ //$NON-NLS-2$

                EntityCollectionResponse result = new EntityCollectionResponse(
                        request.getODataRequest().getRawBaseUri(),
                        visitor.getContext());

                getClient().executeSQL(query, visitor.getParameters(), false, null, null, null, 1, result);

                if (!result.getEntities().isEmpty()) {
                    entity = result.getEntities().get(0);
                    String location = EntityResponse.buildLocation(request.getODataRequest().getRawBaseUri(),
                            entity,
                            request.getEntitySet().getName(),
                            entityType);
                    URI id = new URI(location);
                    entity.setId(id);

                    //handle adding the navigation
                    if (!request.getNavigations().isEmpty()) {
                        String refRequest = request.getODataRequest().getRawRequestUri() + "/$ref"; //$NON-NLS-1$

                        DataRequest bindingRequest;
                        try {
                            bindingRequest = request.parseLink(new URI(refRequest));
                        } catch (URISyntaxException e) {
                            throw new ODataApplicationException(e.getMessage(),
                                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                                    Locale.getDefault(), e);
                        }

                        //TODO: consider wiring this in directly - it could be part of the insert as well
                        manageReferenceInternal(bindingRequest, request.getODataRequest().getRawBaseUri(), id, false);
                    }
                }

                response.writeCreatedEntity(edmEntitySet, entity);
            }
            else {
                response.writeNotModified();
            }
            getClient().commit(txn);
            success = true;
        } catch (EdmPrimitiveTypeException | TeiidException | SQLException | URISyntaxException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        } finally {
            if (!success) {
                try {
                    getClient().rollback(txn);
                } catch (SQLException e1) {
                    // ignore
                }
            }
        }
    }

    @Override
    public void updateEntity(DataRequest request, Entity entity, boolean merge,
            String entityETag, EntityResponse response)
            throws ODataLibraryException, ODataApplicationException {

        // TODO: need to match entityETag.
        checkETag(entityETag);

        UpdateResponse updateResponse = null;
        if (merge) {
            try {
                ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                        getClient().getMetadataStore(), this.prepared, false,
                        request.getODataRequest().getRawBaseUri(), this.serviceMetadata);
                visitor.visit(request.getUriInfo());
                EdmEntityType entityType = request.getEntitySet().getEntityType();
                Update update = visitor.update(entityType, entity, this.prepared);
                updateResponse = getClient().executeUpdate(update, visitor.getParameters());
            } catch (SQLException e) {
                throw new ODataApplicationException(e.getMessage(),
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                        Locale.getDefault(), e);
            }  catch (TeiidException e) {
                throw new ODataApplicationException(e.getMessage(),
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                        Locale.getDefault(), e);
            }
        }
        else {
            // delete, then insert
            String txn = startTransaction();
            boolean success = false;
            try {
                // build insert first as it could fail to validate
                ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                        getClient().getMetadataStore(), this.prepared, false,
                        request.getODataRequest().getRawBaseUri(),
                        this.serviceMetadata);
                visitor.visit(request.getUriInfo());

                EdmEntityType entityType = request.getEntitySet().getEntityType();
                List<UriParameter> keys = request.getKeyPredicates();
                Insert command = visitor.insert(entityType, entity, keys, this.prepared);

                //run delete
                ODataSQLBuilder deleteVisitor = new ODataSQLBuilder(this.odata,
                        getClient().getMetadataStore(), this.prepared, false,
                        request.getODataRequest().getRawBaseUri(),
                        this.serviceMetadata);
                deleteVisitor.visit(request.getUriInfo());
                Delete delete = deleteVisitor.delete();
                updateResponse = getClient().executeUpdate(delete, deleteVisitor.getParameters());

                //run insert
                updateResponse = getClient().executeUpdate(command, visitor.getParameters());
                commit(txn);
                success = true;
            } catch (SQLException e) {
                throw new ODataApplicationException(e.getMessage(),
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                        Locale.getDefault(), e);
            } catch (TeiidException e) {
                throw new ODataApplicationException(e.getMessage(),
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                        Locale.getDefault(), e);
            } finally {
                if (!success) {
                    rollback(txn);
                }
            }
        }

        if (updateResponse!= null && updateResponse.getUpdateCount()  > 0) {
            response.writeUpdatedEntity();
        }
        else {
            response.writeNotModified();
        }
    }

    private void checkETag(String entityETag) throws ODataApplicationException{
        if (entityETag != null && !entityETag.equals("*")) {
            throw new ODataApplicationException(
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16030),
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                    Locale.getDefault());
        }
    }

    @Override
    public void deleteEntity(DataRequest request, String entityETag,
            EntityResponse response) throws ODataLibraryException, ODataApplicationException {

        // TODO: need to match entityETag.
        checkETag(entityETag);

        UpdateResponse updateResponse = null;
        try {
            ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                    getClient().getMetadataStore(), this.prepared, false,
                    request.getODataRequest().getRawBaseUri(), this.serviceMetadata);
            visitor.visit(request.getUriInfo());
            Delete delete = visitor.delete();
            updateResponse = getClient().executeUpdate(delete, visitor.getParameters());
        } catch (SQLException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }

        if (updateResponse != null && updateResponse.getUpdateCount()  > 0) {
            response.writeDeletedEntityOrReference();
        }
        else {
            //since DELETE is idempotent same response as otherwise success operation.
            response.writeDeletedEntityOrReference();
        }
    }

    /**
     * since Teiid only deals with primitive types, merge does not apply
     */
    @Override
    public void updateProperty(DataRequest request, Property property, boolean rawValue,
            boolean merge, String entityETag, PropertyResponse response)
            throws ODataLibraryException, ODataApplicationException {

        // TODO: need to match entityETag.
        checkETag(entityETag);

        UpdateResponse updateResponse = null;
        EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
        try {
            ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                    getClient().getMetadataStore(), this.prepared, false,
                    request.getODataRequest().getRawBaseUri(),this.serviceMetadata);
            visitor.visit(request.getUriInfo());
            Update update = visitor.updateProperty(edmProperty, property, this.prepared, rawValue);
            updateResponse = getClient().executeUpdate(update, visitor.getParameters());
        } catch (SQLException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        } catch (TeiidException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }

        if (updateResponse != null && updateResponse.getUpdateCount() > 0) {
            response.writePropertyUpdated();
        } else {
            response.writeNotModified();
        }
    }

    @Override
    public void upsertStreamProperty(DataRequest request, String entityETag,
            InputStream streamContent, NoContentResponse response)
            throws ODataLibraryException, ODataApplicationException {
        UpdateResponse updateResponse = null;
        EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
        try {
            ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                    getClient().getMetadataStore(), this.prepared, false,
                    request.getODataRequest().getRawBaseUri(),this.serviceMetadata);
            visitor.visit(request.getUriInfo());
            Update update = visitor.updateStreamProperty(edmProperty, streamContent);
            updateResponse = getClient().executeUpdate(update, visitor.getParameters());
        } catch (SQLException | TeiidException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }
        if (updateResponse != null && updateResponse.getUpdateCount() > 0) {
            response.writeNoContent();
        } else {
            response.writeNotModified();
        }
    }

    interface OperationParameterValueProvider {
        Object getValue(EdmParameter parameter, Class<?> runtimeType) throws TeiidProcessingException;
    }

    @Override
    public <T extends ServiceResponse> void invoke(final FunctionRequest request,
            HttpMethod method, T response) throws ODataLibraryException,
            ODataApplicationException {
        invokeOperation(request, new FunctionParameterValueProvider(request.getParameters()), response);
    }

    @Override
    public <T extends ServiceResponse> void invoke(final ActionRequest request,
            String eTag, T response) throws ODataLibraryException, ODataApplicationException {
        checkETag(eTag);
        invokeOperation(request, new ActionParameterValueProvider(request.getPayload(), request), response);
    }

    private <T extends ServiceResponse> void invokeOperation(
            final OperationRequest request,
            OperationParameterValueProvider parameters, T response)
            throws ODataApplicationException, ODataLibraryException {

        checkExpand(request.getUriInfo().asUriInfoResource());

        final ODataSQLBuilder visitor = new ODataSQLBuilder(odata,
                getClient().getMetadataStore(), this.prepared, true,
                request.getODataRequest().getRawBaseUri(), this.serviceMetadata);
        visitor.setOperationParameterValueProvider(parameters);
        visitor.visit(request.getUriInfo());

        final OperationResponseImpl queryResponse;
        try {
            if (visitor.getContext() instanceof NoDocumentNode) {
                NoDocumentNode cdn = (NoDocumentNode)visitor.getContext();
                ProcedureReturn procReturn = cdn.getProcedureReturn();
                queryResponse = new OperationResponseImpl(procReturn);
                getClient().executeCall(cdn.getQuery(), visitor.getParameters(), procReturn, queryResponse);

            } else {
                Query query = visitor.selectQuery();
                queryResponse = (OperationResponseImpl)executeQuery(request, request.isCountRequest(), visitor, query);
            }
        } catch (ODataApplicationException|ODataLibraryException e) {
            throw e;
        } catch (Throwable e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }

        final OperationResponseImpl operationResult = queryResponse;

        if (operationResult.getProcedureReturn().getReturnType() == null) {
            response.writeNoContent(true);
            return;
        }

        if (operationResult.getProcedureReturn().hasResultSet()
                && operationResult.getNextToken() != null) {

            ContextURL.Builder builder = new ContextURL.Builder()
                    .asCollection()
                    .entitySetOrSingletonOrType("Edm.ComplexType");
            EdmComplexResponse complexResponse = EdmComplexResponse.getInstance(
                    request, builder.build(), false, response.getODataResponse());

            sendResults(request, queryResponse, complexResponse);
            return;
        }

        response.accepts(new ServiceResponseVisior() {
            @Override
            public void visit(PropertyResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                Property property = operationResult.getResult();
                Object value = property.getValue();
                if (value instanceof SQLXML || value instanceof Blob || value instanceof Clob) {
                    try {
                        handleLobResult(getClient().getProperty(Client.CHARSET), value, response);
                    } catch (SQLException e) {
                        LogManager.logDetail(LogConstants.CTX_ODATA, e);
                        response.writeServerError(true);
                    }
                }
                else {
                    response.writeProperty(request.getReturnType().getType(), property);
                }
            }
        });
    }

    @Override
    public void readMediaStream(MediaRequest request, StreamResponse response)
            throws ODataLibraryException, ODataApplicationException {
        response.writeServerError(true);
    }

    @Override
    public void upsertMediaStream(MediaRequest request, String entityETag,
            InputStream mediaContent, NoContentResponse response)
            throws ODataLibraryException, ODataApplicationException {
        response.writeServerError(true);
    }

    @Override
    public void anyUnsupported(ODataRequest request, ODataResponse response)
            throws ODataLibraryException, ODataApplicationException {
        throw new ODataApplicationException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16049),
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                Locale.getDefault());
    }

    @Override
    public void addReference(DataRequest request, String entityETag,
            URI referenceId, NoContentResponse response)
            throws ODataLibraryException, ODataApplicationException {
        manageReference(request, referenceId, response, false);
    }

    @Override
    public void updateReference(DataRequest request, String entityETag,
            URI referenceId, NoContentResponse response)
            throws ODataLibraryException, ODataApplicationException {
        manageReference(request, referenceId, response, false);
    }

    private void manageReference(DataRequest request, URI referenceId,
            NoContentResponse response, boolean delete) throws ODataApplicationException {
        UpdateResponse updateResponse = manageReferenceInternal(request, request.getODataRequest().getRawBaseUri(),
                referenceId, delete);

        if (updateResponse != null && updateResponse.getUpdateCount() > 0) {
            response.writeNoContent();
        }
        else {
            response.writeNotModified();
        }
    }

    private UpdateResponse manageReferenceInternal(DataRequest request, String baseUri,
            URI referenceId, boolean delete) throws ODataApplicationException {
        UpdateResponse updateResponse = null;
        try {
            ReferenceUpdateSQLBuilder visitor = new ReferenceUpdateSQLBuilder(getClient().getMetadataStore(),
                    baseUri, this.serviceMetadata, this.odata);
            visitor.visit(request.getUriInfo());

            Update update = visitor.updateReference(referenceId, this.prepared, delete);
            updateResponse = getClient().executeUpdate(update, visitor.getParameters());
        } catch (SQLException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }
        return updateResponse;
    }

    @Override
    public void deleteReference(DataRequest request, URI deleteId,
            String entityETag, NoContentResponse response)
            throws ODataLibraryException, ODataApplicationException {
        manageReference(request, deleteId, response, true);
    }

    @Override
    public String startTransaction() throws ODataLibraryException, ODataApplicationException {
        try {
            return getClient().startTransaction();
        } catch (SQLException e) {
            throw new ODataApplicationException(
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16039),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
        }
    }

    @Override
    public void commit(String txnId) throws ODataLibraryException, ODataApplicationException {
        try {
            getClient().commit(txnId);
        } catch (SQLException e) {
            throw new ODataApplicationException(
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16039),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
        }
    }

    @Override
    public void rollback(String txnId) throws ODataLibraryException, ODataApplicationException {
        try {
            getClient().rollback(txnId);
        } catch (SQLException e) {
            throw new ODataApplicationException(
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16039),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.getDefault());
        }
    }

    @Override
    public void crossJoin(DataRequest request, List<String> entitySetNames,
            ODataResponse response) throws ODataLibraryException,
            ODataApplicationException {

        final ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                getClient().getMetadataStore(), this.prepared, true, request
                .getODataRequest().getRawBaseUri(), this.serviceMetadata);
        visitor.visit(request.getUriInfo());

        try {
            Query query = visitor.selectQuery();
            ComplexResponse queryResponse = (ComplexResponse)executeQuery(request, request.isCountRequest(), visitor, query);
            ContextURL.Builder builder = new ContextURL.Builder()
                .asCollection()
                .entitySetOrSingletonOrType("Edm.ComplexType");

            EdmComplexResponse complexResponse = EdmComplexResponse.getInstance(
                    request, builder.build(), false, response);
            sendResults(request, queryResponse, complexResponse);
        } catch (ODataApplicationException e) {
            throw e;
        } catch (ODataLibraryException e) {
            throw e;
        } catch (Exception e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }
    }

    public void apply(DataRequest request, ODataResponse response)
            throws ODataLibraryException, ODataApplicationException {
        final ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                getClient().getMetadataStore(), this.prepared, true, request
                .getODataRequest().getRawBaseUri(), this.serviceMetadata);
        visitor.visit(request.getUriInfo());

        try {
            Query query = visitor.selectQuery();
            ApplyResult queryResponse = (ApplyResult)executeQuery(request, request.isCountRequest(), visitor, query);
            ApplyDocumentNode adn = queryResponse.getDocumentNode();
            DocumentNode dn = adn.getBaseContext();
            ContextURL.Builder builder = new ContextURL.Builder();
            if (dn instanceof CrossJoinNode) {
                builder = builder.asCollection()
                        .entitySetOrSingletonOrType("Edm.ComplexType"); //$NON-NLS-1$
            } else {
                String columns = adn.getAllProjectedColumns().stream()
                        .map(p -> p.getName())
                        .collect(Collectors.joining(",")); //$NON-NLS-1$
                builder = builder.entitySetOrSingletonOrType(dn.getEdmStructuredType().getName()+"(" + columns + ")"); //$NON-NLS-1$ //$NON-NLS-2$
            }

            EdmComplexResponse complexResponse = EdmComplexResponse.getInstance(
                    request, builder.build(), false, response);
            sendResults(request, queryResponse, complexResponse);
        } catch (ODataApplicationException e) {
            throw e;
        } catch (ODataLibraryException e) {
            throw e;
        } catch (Exception e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }
    }

    private void handleLobResult(String charSet, Object result, ServiceResponse response) throws SQLException {
        if (result == null) {
            return; //or should this be an empty result?
        }

        if (result instanceof SQLXML) {
            if (charSet != null) {
                XMLSerialize serialize = new XMLSerialize();
                serialize.setTypeString("blob"); //$NON-NLS-1$
                serialize.setDeclaration(true);
                serialize.setEncoding(charSet);
                serialize.setDocument(true);
                try {
                    InputStream content = ((BlobType)XMLSystemFunctions.serialize(serialize, new XMLType((SQLXML)result))).getBinaryStream();
                    response.writeContent(content, 200, false);
                    response.writeOK(ContentType.APPLICATION_OCTET_STREAM);
                } catch (TransformationException e) {
                    throw new SQLException(e);
                }
            }
            else {
                InputStream content = ((SQLXML)result).getBinaryStream();
                response.writeContent(content, 200, false);
                response.writeOK(ContentType.APPLICATION_XML);
            }
        }
        else if (result instanceof Blob) {
            InputStream content =  ((Blob)result).getBinaryStream();
            response.writeContent(content, 200, false);
            response.writeOK(ContentType.APPLICATION_OCTET_STREAM);
        }
        else if (result instanceof Clob) {
            InputStream content =  new ReaderInputStream(((Clob)result).getCharacterStream(), charSet==null?Charset.defaultCharset():Charset.forName(charSet));
            response.writeContent(content, 200, false);
            response.writeOK(ContentType.TEXT_PLAIN);
        }
        else {
            InputStream content =  new ByteArrayInputStream(result.toString().getBytes(charSet==null?Charset.defaultCharset():Charset.forName(charSet)));
            response.writeContent(content, 200, false);
            response.writeOK(ContentType.APPLICATION_OCTET_STREAM);
        }
    }

    @Override
    public void upsertEntity(DataRequest request, Entity entity, boolean merge,
            String entityETag, EntityResponse response)
            throws ODataLibraryException, ODataApplicationException {

        final ODataSQLBuilder visitor = new ODataSQLBuilder(this.odata,
                getClient().getMetadataStore(), this.prepared, true,
                request.getODataRequest().getRawBaseUri(), this.serviceMetadata);
        visitor.visit(request.getUriInfo());

        final EntityCollectionResponse queryResponse;
        try {
            Query query = visitor.selectQuery();
            queryResponse = (EntityCollectionResponse)executeQuery(request, request.isCountRequest(), visitor, query);
        } catch (ODataApplicationException|ODataLibraryException e) {
            throw e;
        } catch (Exception e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }

        if (!queryResponse.getEntities().isEmpty()) {
            updateEntity(request, entity, merge, entityETag, response);
        } else {
            createEntity(request, entity, response);
        }
    }

    @Override
    public boolean supportsDataIsolation() {
        return false;
    }

    @Override
    public void processError(ODataServerError error, ErrorResponse response) {
        int logLevel = error.getStatusCode() >= 500?MessageLevel.ERROR:MessageLevel.WARNING;
        Throwable ex = getRoot(error.getException());
        //many exceptions in TeiidServiceHandler default as INTERNAL_SERVER_ERROR
        //so we make a better check for codes here
        if (ex instanceof TeiidNotImplementedException) {
            error.setException((TeiidNotImplementedException)ex);
            error.setCode(((TeiidNotImplementedException)ex).getCode());
            error.setStatusCode(501);
            logLevel = MessageLevel.DETAIL;
        } else if (ex instanceof TeiidProcessingException) {
            error.setException((TeiidProcessingException)ex);
            error.setCode(((TeiidProcessingException)ex).getCode());
            Throwable cause = ex.getCause();
            if (cause != null && cause != ex) {
                cause = getRoot(cause);
            }
            if (cause == null || cause instanceof TeiidProcessingException
                    || !(cause instanceof TeiidException || cause instanceof TeiidRuntimeException)) {
                error.setStatusCode(400);
            } else {
                error.setStatusCode(500);
            }
            logLevel = MessageLevel.WARNING;
        } else if (ex instanceof TeiidException) {
            error.setException((TeiidException)ex);
            error.setCode(((TeiidException)ex).getCode());
            error.setStatusCode(500);
            logLevel = MessageLevel.ERROR;
        } else if (ex instanceof TeiidRuntimeException) {
            error.setException((TeiidRuntimeException)ex);
            error.setCode(((TeiidRuntimeException)ex).getCode());
            error.setStatusCode(500);
            logLevel = MessageLevel.ERROR;
        }

        if (ex != error.getException() && ex.getMessage() != null) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODATA, MessageLevel.DETAIL) || logLevel <= MessageLevel.ERROR) {
                LogManager.log(logLevel, LogConstants.CTX_ODATA, error.getException(), ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16050, error.getMessage(), ex.getMessage()));
            } else {
                LogManager.log(logLevel, LogConstants.CTX_DQP, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16051, error.getMessage(), ex.getMessage()));
            }
        } else {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODATA, MessageLevel.DETAIL) || logLevel <= MessageLevel.ERROR) {
                LogManager.log(logLevel, LogConstants.CTX_ODATA, error.getException(), ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16052, error.getMessage()));
            } else {
                LogManager.log(logLevel, LogConstants.CTX_DQP, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16053, error.getMessage()));
            }
        }
        response.writeError(error);
    }

    private Throwable getRoot(Throwable t) {
        if (t.getCause() != null && t.getCause() != t) {
            if (t.getCause() instanceof TeiidException || t.getCause() instanceof TeiidRuntimeException) {
                return t.getCause();
            }
            return getRoot(t.getCause());
        }
        return t;
    }
}
