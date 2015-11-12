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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.core.ServiceHandler;
import org.apache.olingo.server.core.deserializer.json.ODataJsonDeserializer;
import org.apache.olingo.server.core.requests.ActionRequest;
import org.apache.olingo.server.core.requests.DataRequest;
import org.apache.olingo.server.core.requests.FunctionRequest;
import org.apache.olingo.server.core.requests.MediaRequest;
import org.apache.olingo.server.core.requests.MetadataRequest;
import org.apache.olingo.server.core.requests.OperationRequest;
import org.apache.olingo.server.core.requests.ServiceDocumentRequest;
import org.apache.olingo.server.core.responses.CountResponse;
import org.apache.olingo.server.core.responses.EntityResponse;
import org.apache.olingo.server.core.responses.EntitySetResponse;
import org.apache.olingo.server.core.responses.MetadataResponse;
import org.apache.olingo.server.core.responses.NoContentResponse;
import org.apache.olingo.server.core.responses.PrimitiveValueResponse;
import org.apache.olingo.server.core.responses.PropertyResponse;
import org.apache.olingo.server.core.responses.ServiceDocumentResponse;
import org.apache.olingo.server.core.responses.ServiceResponse;
import org.apache.olingo.server.core.responses.ServiceResponseVisior;
import org.apache.olingo.server.core.responses.StreamResponse;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidException;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.odata.api.BaseResponse;
import org.teiid.odata.api.Client;
import org.teiid.odata.api.QueryResponse;
import org.teiid.odata.api.UpdateResponse;
import org.teiid.olingo.EdmComplexResponse;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.service.ProcedureSQLBuilder.ProcedureReturn;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.XMLSerialize;

public class TeiidServiceHandler implements ServiceHandler {
    private static final String PREFERENCE_APPLIED = "Preference-Applied";
    private static final String ODATA_MAXPAGESIZE = "odata.maxpagesize";
    private boolean prepared = true;
    private OData odata;
    private ServiceMetadata serviceMetadata;
    private String schemaName;
    private UniqueNameGenerator nameGenerator = new UniqueNameGenerator();
    
    private static ThreadLocal<Client> CLIENT = new ThreadLocal<Client>() {
        @Override
        protected Client initialValue() {
            return null;
        }
    };

    public static Client getClient() {
        return CLIENT.get();
    }

    public static void setClient(Client client) {
        CLIENT.set(client);
    }    
    
    public TeiidServiceHandler(String schemaName) {
        this.schemaName = schemaName;
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

    class UniqueNameGenerator {
        private final AtomicInteger groupCount = new AtomicInteger(0);
        public String getNextGroup() {
            String aliasGroup = "g"+this.groupCount.getAndIncrement(); //$NON-NLS-1$
            return aliasGroup;
        }
    }    
    
    @Override
    public <T extends ServiceResponse> void read(final DataRequest request, T response)
            throws ODataLibraryException, ODataApplicationException {
        
        final ODataSQLBuilder visitor = new ODataSQLBuilder(
                getClient().getMetadataStore(), this.prepared, true, 
                request.getODataRequest().getRawBaseUri(), this.serviceMetadata, this.nameGenerator);
        visitor.visit(request.getUriInfo());
        
        final BaseResponse queryResponse;
        try {
            Query query = visitor.selectQuery();
            queryResponse = executeQuery(request, visitor, query);
        } catch (Exception e) {
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
                Entity entity = entitySet.getEntities().get(0);
                
                EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
                Property property = entity.getProperty(edmProperty.getName());
                if (property == null) {
                    response.writeNotFound(true);
                }
                else if (property.getValue() == null) {
                    response.writeNoContent(true);
                }
                response.write(property.getValue());            
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
                EntityCollection entitySet = (EntityCollection)queryResponse;
                Entity entity = entitySet.getEntities().get(0);
                
                EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
                Property property = entity.getProperty(edmProperty.getName());
                Object value = property.getValue();
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
                    response.writeNoContent(true);
                } else {
                    response.writeReadEntity(visitor.getContext().getEdmEntityType(), 
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
            } catch (MalformedURLException e) {
                throw new ODataApplicationException(e.getMessage(), 500, Locale.getDefault(), e);
            }
        }
        response.writeReadEntitySet(visitor.getContext().getEdmEntityType(), result);
    }

    String buildNextToken(final String queryPath, String nextToken)
            throws URISyntaxException, MalformedURLException {
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
    
    private void sendResults(final DataRequest request,
            final ODataSQLBuilder visitor,
            final BaseResponse queryResponse, EdmComplexResponse response)
            throws ODataApplicationException, SerializerException {
        if (request.getPreference(ODATA_MAXPAGESIZE) != null) {
            response.writeHeader(PREFERENCE_APPLIED,
                    ODATA_MAXPAGESIZE+"="+ request.getPreference(ODATA_MAXPAGESIZE)); //$NON-NLS-1$
        }
        CrossJoinResult result = (CrossJoinResult)queryResponse;
        URI next = null;
        if (result.getNextToken() != null) {
            try {
                next = new URI(request.getODataRequest().getRawRequestUri()
                        + (request.getODataRequest().getRawQueryPath() == null ?"?$skiptoken=":"&$skiptoken=")
                        + result.getNextToken());
            } catch (URISyntaxException e) {
                throw new ODataApplicationException(e.getMessage(), 500, Locale.getDefault(), e);
            }
        }
        response.writeComplexType(result, next);
    }    

    private BaseResponse executeQuery(final DataRequest request,
            final ODataSQLBuilder visitor, Query query) throws SQLException {
        if (request.isCountRequest()) {
            return getClient().executeCount(query, visitor.getParameters());
        }
        else {
            String pageSize = request.getPreference(ODATA_MAXPAGESIZE);
            if (pageSize == null) {
                if (getClient().getProperty(Client.BATCH_SIZE) == null) {
                    pageSize = String.valueOf(BufferManagerImpl.DEFAULT_PROCESSOR_BATCH_SIZE);
                }
                else {
                    pageSize = getClient().getProperty(Client.BATCH_SIZE);
                }
            }

            QueryResponse result = new EntityCollectionResponse(getClient().getProperty(Client.INVALID_CHARACTER_REPLACEMENT),
                    visitor.getContext());
            
            if (visitor.getContext() instanceof CrossJoinNode) {
                result = new CrossJoinResult(getClient().getProperty(Client.INVALID_CHARACTER_REPLACEMENT),
                        (CrossJoinNode)visitor.getContext());
            }

            getClient().executeSQL(query, visitor.getParameters(),
                    visitor.includeTotalSize(), visitor.getSkip(),
                    visitor.getTop(), visitor.getNextToken(), Integer.parseInt(pageSize), result);
            
            return result;
        }
    }

    private void checkExpand(UriInfoResource queryInfo) {
        if (queryInfo.getExpandOption() != null && !queryInfo.getExpandOption().getExpandItems().isEmpty()) {
            throw new UnsupportedOperationException("Expand is not supported"); //$NON-NLS-1$
        }
    }
    
    private UpdateResponse performInsert(String rawURI, UriInfo uriInfo,
            EdmEntityType entityType, Entity entity) throws SQLException, TeiidException {
        ODataSQLBuilder visitor = new ODataSQLBuilder(getClient().getMetadataStore(), this.prepared, false,
                rawURI, this.serviceMetadata, this.nameGenerator);
        visitor.visit(uriInfo);
        Insert command = visitor.insert(entityType, entity, this.prepared);
        return getClient().executeUpdate(command, visitor.getParameters());
    }
    
    private UpdateResponse performDeepInsert(String rawURI, UriInfo uriInfo,
            EdmEntityType entityType, Entity entity) throws SQLException, TeiidException {
        String txn = getClient().startTransaction();
        try {
            UpdateResponse response = performInsert(rawURI, uriInfo, entityType, entity);
            for (String navigationName:entityType.getNavigationPropertyNames()) {
                EdmNavigationProperty navProperty = entityType.getNavigationProperty(navigationName);
                Link navLink = entity.getNavigationLink(navigationName);
                if (navLink != null && navLink.getInlineEntity() != null) {
                    performInsert(rawURI, uriInfo, navProperty.getType(), navLink.getInlineEntity());
                }
                if (navLink != null && navLink.getInlineEntitySet() != null && !navLink.getInlineEntitySet().getEntities().isEmpty()) {
                    for (Entity inlineEntity:navLink.getInlineEntitySet().getEntities()) {
                        performInsert(rawURI, uriInfo, navProperty.getType(), inlineEntity);
                    }
                }
            }
            getClient().commit(txn);
            return response;
        } catch (SQLException e) {
            try {
                getClient().rollback(txn);
            } catch (SQLException e1) {
                // ignore
            }
            throw e;
        } catch (TeiidException e) {
            try {
                getClient().rollback(txn);
            } catch (SQLException e1) {
                // ignore
            }
            throw e;            
        }
    }

    private Set<EdmNavigationProperty> deepInsertNames(EdmEntityType entityType, Entity entity) {
        Set<EdmNavigationProperty> expand = new HashSet<EdmNavigationProperty>();
        
        for (String navigationName:entityType.getNavigationPropertyNames()) {
            EdmNavigationProperty navProperty = entityType.getNavigationProperty(navigationName);
            Link navLink = entity.getNavigationLink(navigationName);
            if (navLink != null) {
                if (navLink.getInlineEntity() != null) {
                   expand.add(navProperty);
                }
                if (navLink.getInlineEntitySet() != null && !navLink.getInlineEntitySet().getEntities().isEmpty()) {
                    expand.add(navProperty);
                }
            }
        }
        return expand;
    }
    
    @Override
    public void createEntity(DataRequest request, Entity entity,
            EntityResponse response) throws ODataLibraryException,
            ODataApplicationException {
        
        try {
            EdmEntityType entityType = request.getEntitySet().getEntityType();
            UpdateResponse updateResponse = performDeepInsert(request
                    .getODataRequest().getRawBaseUri(), request.getUriInfo(),
                    entityType, entity);
            
            if (updateResponse.getUpdateCount()  == 1) {
                ODataSQLBuilder visitor = new ODataSQLBuilder(getClient().getMetadataStore(), true, false, 
                        request.getODataRequest().getRawBaseUri(), this.serviceMetadata, this.nameGenerator);
                
                Query query = visitor.selectWithEntityKey(entityType,
                                entity, updateResponse.getGeneratedKeys(), deepInsertNames(entityType, entity));
                LogManager.logDetail(LogConstants.CTX_ODATA, null, "created entity = ", entityType.getName(), " with key=", query.getCriteria().toString()); //$NON-NLS-1$ //$NON-NLS-2$
                
                EntityCollectionResponse result = new EntityCollectionResponse(getClient().getProperty(
                        Client.INVALID_CHARACTER_REPLACEMENT),
                        visitor.getContext());
                
                getClient().executeSQL(query, visitor.getParameters(), false, null, null, null, 1, result);
                
                if (!result.getEntities().isEmpty()) {
                    entity = result.getEntities().get(0);
                    String location = EntityResponse.buildLocation(request.getODataRequest().getRawBaseUri(),
                            entity, 
                            request.getEntitySet().getName(), 
                            entityType);
                    entity.setId(new URI(location));
                }
                response.writeCreatedEntity(request.getEntitySet(), entity);
            }
            else {
                response.writeNotModified();
            }
        } catch (SQLException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        } catch (URISyntaxException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        } catch (TeiidException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
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
                ODataSQLBuilder visitor = new ODataSQLBuilder(getClient().getMetadataStore(), this.prepared, false,
                        request.getODataRequest().getRawBaseUri(), this.serviceMetadata, this.nameGenerator);
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
            try {
                ODataSQLBuilder visitor = new ODataSQLBuilder(getClient().getMetadataStore(), this.prepared, false,
                        request.getODataRequest().getRawBaseUri(), this.serviceMetadata, this.nameGenerator);
                visitor.visit(request.getUriInfo());
                Delete delete = visitor.delete();
                updateResponse = getClient().executeUpdate(delete, visitor.getParameters());
                
                // insert
                ODataJsonDeserializer deserializer = new ODataJsonDeserializer(ContentType.JSON);
                             
                visitor = new ODataSQLBuilder(getClient().getMetadataStore(), this.prepared, false,
                        request.getODataRequest().getRawBaseUri(), this.serviceMetadata, this.nameGenerator);
                visitor.visit(request.getUriInfo());
                
                EdmEntityType entityType = request.getEntitySet().getEntityType();
                List<UriParameter> keys = request.getKeyPredicates();
                for (UriParameter key : keys) {
                    EdmProperty edmProperty = (EdmProperty)entityType.getProperty(key.getName());
                    Property property = deserializer.property(
                            new ByteArrayInputStream(key.getText().getBytes()),
                            edmProperty).getProperty();
                    entity.addProperty(property);
                }
                Insert command = visitor.insert(entityType, entity, this.prepared);
                updateResponse = getClient().executeUpdate(command, visitor.getParameters());
                commit(txn);
            } catch (SQLException e) {
                rollback(txn);
                throw new ODataApplicationException(e.getMessage(),
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                        Locale.getDefault(), e);                
            } catch (TeiidException e) {
                rollback(txn);
                throw new ODataApplicationException(e.getMessage(),
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                        Locale.getDefault(), e);            
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
            ODataSQLBuilder visitor = new ODataSQLBuilder(getClient().getMetadataStore(), this.prepared, false,
                    request.getODataRequest().getRawBaseUri(), this.serviceMetadata, this.nameGenerator);
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
            response.writeNotModified();
        }
    }

    /**
     * since Teiid only deals with primitive types, merge does not apply
     */
    @Override
    public void updateProperty(DataRequest request, Property property,
            boolean merge, String entityETag, PropertyResponse response)
            throws ODataLibraryException, ODataApplicationException {

        // TODO: need to match entityETag.
        checkETag(entityETag);
        
        UpdateResponse updateResponse = null;
        EdmProperty edmProperty = request.getUriResourceProperty().getProperty();
        try {
            ODataSQLBuilder visitor = new ODataSQLBuilder(getClient().getMetadataStore(), this.prepared, false,
                    request.getODataRequest().getRawBaseUri(),this.serviceMetadata, this.nameGenerator);
            visitor.visit(request.getUriInfo());
            Update update = visitor.updateProperty(edmProperty, property, this.prepared);
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
            ODataSQLBuilder visitor = new ODataSQLBuilder(getClient().getMetadataStore(), this.prepared, false,
                    request.getODataRequest().getRawBaseUri(),this.serviceMetadata, this.nameGenerator);
            visitor.visit(request.getUriInfo());
            Update update = visitor.updateStreamProperty(edmProperty, streamContent);
            updateResponse = getClient().executeUpdate(update, visitor.getParameters());
        } catch (SQLException e) {
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

    @Override
    public <T extends ServiceResponse> void invoke(final FunctionRequest request,
            HttpMethod method, T response) throws ODataLibraryException,
            ODataApplicationException {
        invokeOperation(request, response);
    }
    
    @Override
    public <T extends ServiceResponse> void invoke(final ActionRequest request,
            String eTag, T response) throws ODataLibraryException, ODataApplicationException {
        checkETag(eTag);        
        invokeOperation(request, response);
    }    
        
    private <T extends ServiceResponse> void invokeOperation(final OperationRequest request,
            T response) throws ODataApplicationException, ODataLibraryException {
        
        checkExpand(request.getUriInfo().asUriInfoResource());
        
        OperationResponseImpl result = null;
        try {
            MetadataStore store = getClient().getMetadataStore();
            ProcedureSQLBuilder builder = new ProcedureSQLBuilder(store.getSchema(schemaName), request);
            ProcedureReturn procedureReturn = builder.getReturn();
            result = new OperationResponseImpl(
                    getClient().getProperty(Client.INVALID_CHARACTER_REPLACEMENT), 
                    procedureReturn.getReturnType());
            
            getClient().executeCall(builder.buildProcedureSQL(), builder.getSqlParameters(), procedureReturn, result);
        } catch (SQLException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        } catch (TeiidException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        } 
        
        final OperationResponseImpl operationResult = result;
        response.accepts(new ServiceResponseVisior() {
            @Override
            public void visit(PropertyResponse response)
                    throws ODataLibraryException, ODataApplicationException {
                Property property = (Property)operationResult.getResult();
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
        response.setStatusCode(500);
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
        UpdateResponse updateResponse = null;
        try {
            ReferenceUpdateSQLBuilder visitor = new ReferenceUpdateSQLBuilder(getClient().getMetadataStore(), 
                    request.getODataRequest().getRawBaseUri(), this.serviceMetadata);
            visitor.visit(request.getUriInfo());

            Update update = visitor.updateReference(referenceId, this.prepared, delete);
            updateResponse = getClient().executeUpdate(update, visitor.getParameters());
        } catch (SQLException e) {
            throw new ODataApplicationException(e.getMessage(),
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                    Locale.getDefault(), e);
        }
        
        if (updateResponse != null && updateResponse.getUpdateCount() > 0) {
            response.writeNoContent();
        }
        else {
            response.writeNotModified();
        }
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
        
        final ODataSQLBuilder visitor = new ODataSQLBuilder(getClient()
                .getMetadataStore(), this.prepared, true, request
                .getODataRequest().getRawBaseUri(), this.serviceMetadata,
                this.nameGenerator);
        visitor.visit(request.getUriInfo());

        try {
            Query query = visitor.selectQuery();
            BaseResponse queryResponse = executeQuery(request, visitor, query);
            ContextURL.Builder builder = new ContextURL.Builder()
                .asCollection()
                .entitySetOrSingletonOrType("Edm.ComplexType");
                
            EdmComplexResponse complexResponse = EdmComplexResponse.getInstance(
                    request, builder.build(), false, response);
            sendResults(request, visitor, queryResponse, complexResponse);
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
        
        final ODataSQLBuilder visitor = new ODataSQLBuilder(
                getClient().getMetadataStore(), this.prepared, true, 
                request.getODataRequest().getRawBaseUri(), this.serviceMetadata, this.nameGenerator);
        visitor.visit(request.getUriInfo());
        
        final EntityCollectionResponse queryResponse;
        try {
            Query query = visitor.selectQuery();
            queryResponse = (EntityCollectionResponse)executeQuery(request, visitor, query);
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
}
