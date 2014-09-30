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

import java.io.ByteArrayInputStream;
import java.util.List;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.ContextURL.Suffix;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.format.ODataFormat;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.processor.CountProcessor;
import org.apache.olingo.server.api.processor.DefaultProcessor;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.ODataSerializerException;
import org.apache.olingo.server.api.serializer.ODataSerializerOptions;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.teiid.core.TeiidException;
import org.teiid.query.sql.lang.Query;

public class TeiidProcessor extends DefaultProcessor implements
        EntityCollectionProcessor, EntityProcessor, CountProcessor {
    private final Client client;
    private final boolean prepared;
    private OData odata;
    private Edm edm;

    public TeiidProcessor(Client client, boolean prepared) {
        this.client = client;
        this.prepared = prepared;
    }

    @Override
    public void init(final OData odata, final Edm edm) {
        super.init(odata, edm);
        this.odata = odata;
        this.edm = edm;
    }

    @Override
    public void readCollection(ODataRequest request, ODataResponse response,UriInfo uriInfo, ContentType format) {
        readEntitySet(response, uriInfo, format, false);
    }

    private void readEntitySet(ODataResponse response, UriInfo uriInfo, ContentType contentType, boolean singleRow) {
        try {
            checkExpand(uriInfo.asUriInfoResource());
            ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), this.prepared);
            visitor.visit(uriInfo);
            Query query = visitor.selectQuery(false);
            List<SQLParam> parameters = visitor.getParameters();

            EntityList result = new EntityList(client.getProperty(LocalClient.INVALID_CHARACTER_REPLACEMENT),
                    visitor.getEntitySet(), visitor.getProjectedColumns());

            this.client.executeSQL(query, parameters, visitor.isCountQuery(),visitor.getSkip(), visitor.getTop(), result);
            if (singleRow && result.getEntities().isEmpty()){
                response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
            }
            else {
                ODataFormat format = ODataFormat.fromContentType(contentType);
                ODataSerializer serializer = this.odata.createSerializer(format);
                ODataSerializerOptions options = getContextUrl(visitor.getEntitySet(), uriInfo, format,
                        serializer, singleRow, new ContextURLHelper().buildURL(uriInfo));
                response.setContent(serializer.entitySet(visitor.getEntitySet(),result, options));
                response.setStatusCode(HttpStatusCode.OK.getStatusCode());
                response.setHeader(HttpHeader.CONTENT_TYPE,contentType.toContentTypeString());
            }
        } catch (Exception e) {
            handleException(response, contentType, e);
        }
    }

    private void handleException(ODataResponse response, ContentType format, Exception e) {
        try {
            ODataSerializer serializer = this.odata.createSerializer(ODataFormat.fromContentType(format));
            ODataServerError error = new ODataServerError();
            error.setStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
            if (e instanceof TeiidException) {
                error.setCode(((TeiidException) e).getCode());
            }
            error.setException(e);
            serializer.error(error);
        } catch (ODataSerializerException e1) {
            response.setStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    private void checkExpand(UriInfoResource queryInfo) {
        if (queryInfo.getExpandOption() != null && !queryInfo.getExpandOption().getExpandItems().isEmpty()) {
            throw new UnsupportedOperationException("Expand is not supported"); //$NON-NLS-1$
        }
    }

    @Override
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType format) {
        readEntitySet(response, uriInfo, format, true);
    }

    private ODataSerializerOptions getContextUrl( final org.apache.olingo.commons.api.edm.EdmEntitySet entitySet,
            final UriInfo uriInfo,
            final ODataFormat format,
            final ODataSerializer serializer,
            final boolean isSingleEntity,
            final String path) throws ODataSerializerException {
        ContextURL contextUrl = ContextURL.with().entitySetOrSingletonOrType(path)
                .selectList(serializer.buildContextURLSelectList(entitySet, uriInfo.getExpandOption(), uriInfo.getSelectOption()))
                .suffix(isSingleEntity ? Suffix.ENTITY : null)
                .build();
        return ODataSerializerOptions.with()
            .contextURL(format == ODataFormat.JSON_NO_METADATA ? null : contextUrl)
            .count(uriInfo.getCountOption())
            .expand(uriInfo.getExpandOption()).select(uriInfo.getSelectOption())
            .build();
    }

    @Override
    public void readCount(ODataRequest request, ODataResponse response,UriInfo uriInfo) {
        try {
            checkExpand(uriInfo.asUriInfoResource());
            ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), this.prepared);
            visitor.visit(uriInfo);
            Query query = visitor.selectQuery(true);
            List<SQLParam> parameters = visitor.getParameters();

            CountResponse countResponse = this.client.executeCount(query,parameters);
            ByteArrayInputStream bis = new ByteArrayInputStream(String.valueOf(countResponse.getCount()).getBytes());
            response.setContent(bis);
            response.setStatusCode(HttpStatusCode.OK.getStatusCode());
            response.setHeader(HttpHeader.CONTENT_TYPE,ContentType.TEXT_PLAIN.toContentTypeString());
        } catch (Exception e) {
            handleException(response, ContentType.APPLICATION_JSON, e);
        }
    }

    @Override
    public void readEntityProperty(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType contentType, boolean value) {
        try {
            checkExpand(uriInfo.asUriInfoResource());
            ODataSQLBuilder visitor = new ODataSQLBuilder(this.client.getMetadataStore(), this.prepared);
            visitor.visit(uriInfo);
            Query query = visitor.selectQuery(false);
            List<SQLParam> parameters = visitor.getParameters();

            EntityList result = new EntityList(client.getProperty(LocalClient.INVALID_CHARACTER_REPLACEMENT),
                    visitor.getEntitySet(), visitor.getProjectedColumns());

            this.client.executeSQL(query, parameters, visitor.isCountQuery(),visitor.getSkip(), visitor.getTop(), result);
            if (result.getEntities().isEmpty()){
                response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
            }
            else {
                UriResourceProperty uriProperty = (UriResourceProperty) uriInfo.getUriResourceParts().get(uriInfo.getUriResourceParts().size() - 1);
                EdmProperty edmProperty = uriProperty.getProperty();
                Property property = result.getEntities().get(0).getProperty(edmProperty.getName());
                if (property == null) {
                    response.setStatusCode(HttpStatusCode.NOT_FOUND.getStatusCode());
                } else {
                    if (property.isNull()) {
                        response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
                    } else {
                        final ODataFormat format = ODataFormat.fromContentType(contentType);
                        ODataSerializer serializer = odata.createSerializer(format);
                        ODataSerializerOptions options = getContextUrl(visitor.getEntitySet(), uriInfo,
                                format, serializer, false, new ContextURLHelper().buildURL(uriInfo));
                        response.setContent(serializer.entityProperty(edmProperty, property, value, options));
                        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
                        response.setHeader(HttpHeader.CONTENT_TYPE, contentType.toContentTypeString());
                    }
                }
            }
        } catch (Exception e) {
            handleException(response, contentType, e);
        }
    }
}
