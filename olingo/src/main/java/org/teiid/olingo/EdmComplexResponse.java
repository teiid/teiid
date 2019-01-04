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

import java.net.URI;
import java.util.Map;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.core.ContentNegotiatorException;
import org.apache.olingo.server.core.ServiceRequest;
import org.apache.olingo.server.core.responses.ServiceResponse;
import org.apache.olingo.server.core.responses.ServiceResponseVisior;
import org.teiid.odata.api.ComplexResponse;

public class EdmComplexResponse extends ServiceResponse {
    private final TeiidODataJsonSerializer serializer;
    private final ContentType responseContentType;
    private final ContextURL contextURL;
    private EdmComplexResponse(ServiceMetadata metadata,
            ODataResponse response, TeiidODataJsonSerializer serializer,
            ContentType responseContentType, Map<String, String> preferences, ContextURL contextURL) {
        super(metadata, response, preferences);
        this.serializer = serializer;
        this.responseContentType = responseContentType;
        this.contextURL = contextURL;
    }

    public static EdmComplexResponse getInstance(ServiceRequest request,
            ContextURL contextURL, boolean referencesOnly,
            ODataResponse response) throws ContentNegotiatorException,
            SerializerException {
        return new EdmComplexResponse(request.getServiceMetaData(), response,
                new TeiidODataJsonSerializer(request.getResponseContentType()), 
                request.getResponseContentType(), request.getPreferences(), contextURL);
    }

    public void writeComplexType(ComplexResponse complexResult, URI next) throws SerializerException {

        assert (!isClosed());

        if (complexResult == null) {
            writeNotFound(true);
            return;
        }

        // write the whole collection to response
        complexResult.serialize(response, serializer, metadata, contextURL, next);
        
        writeOK(this.responseContentType);
        close();
    }

    @Override
    public void accepts(ServiceResponseVisior visitor)
            throws ODataLibraryException, ODataApplicationException {
    }

    public void writeError(ODataServerError error) {
        try {
            writeContent(this.serializer.error(error).getContent(), error.getStatusCode(), true);
        } catch (SerializerException e) {
            writeServerError(true);
        }
    }
}