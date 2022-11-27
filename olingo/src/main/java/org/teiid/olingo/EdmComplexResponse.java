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
        ContentType contentType = request.getResponseContentType();
        if (!contentType.isCompatible(ContentType.APPLICATION_JSON)) {
            throw new SerializerException("Unsupported format for complex response: " + contentType.toContentTypeString(), //$NON-NLS-1$
                    SerializerException.MessageKeys.UNSUPPORTED_FORMAT, contentType.toContentTypeString());
        }
        return new EdmComplexResponse(request.getServiceMetaData(), response,
                new TeiidODataJsonSerializer(contentType),
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