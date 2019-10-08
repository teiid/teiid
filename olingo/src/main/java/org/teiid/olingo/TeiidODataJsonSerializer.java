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

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.core.serializer.SerializerResultImpl;
import org.apache.olingo.server.core.serializer.json.ODataJsonSerializer;
import org.apache.olingo.server.core.serializer.utils.CircleStreamBuffer;
import org.apache.olingo.server.core.serializer.utils.ContentTypeHelper;
import org.apache.olingo.server.core.serializer.utils.ContextURLBuilder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class TeiidODataJsonSerializer extends ODataJsonSerializer {

    private boolean isODataMetadataFull;
    private boolean isODataMetadataNone;

    public TeiidODataJsonSerializer(ContentType contentType) {
        super(contentType);
        isODataMetadataNone = ContentTypeHelper.isODataMetadataNone(contentType);
        isODataMetadataFull = ContentTypeHelper.isODataMetadataFull(contentType);
    }

    public SerializerResult complexCollection(final ServiceMetadata metadata,
            final List<List<ComplexReturnType>> result,
            final ContextURL contextURL, final URI nextLink) throws SerializerException {
        CircleStreamBuffer buffer = new CircleStreamBuffer();
        try {
            JsonGenerator json = new JsonFactory().createGenerator(buffer.getOutputStream());
            json.writeStartObject();

            if (contextURL != null && (isODataMetadataFull || !isODataMetadataNone)) {
                json.writeStringField(Constants.JSON_CONTEXT, ContextURLBuilder.create(contextURL).toASCIIString());
            }
            json.writeFieldName(Constants.VALUE);
            json.writeStartArray();
            for (List<ComplexReturnType> ct:result) {
                boolean isCross = ct.size() > 1;
                json.writeStartObject();
                for (final ComplexReturnType type : ct) {
                  if (!type.isExpand() && type.getEntity().getId() != null) {
                    json.writeStringField(type.getName()+Constants.JSON_NAVIGATION_LINK, type.getEntity().getId().toASCIIString());
                  }
                  else {
                    if (isCross) {
                        json.writeFieldName(type.getName());
                    }
                    if (type.getEdmStructuredType() instanceof EdmEntityType) {
                        writeEntity(metadata, (EdmEntityType)type.getEdmStructuredType(), type.getEntity(), null, null, null, null, false, null, type.getName(), json);
                    } else {
                        //emit properties based upon what is on the entity, rather than what is on the type
                        //as the dynamic type still defers to the base type
                        if (isCross) {
                            json.writeStartObject();
                        }
                        //no id for aggregate entities
                        json.writeStringField("@odata.id", "null"); //$NON-NLS-1$ //$NON-NLS-2$
                        for (Property property : type.getEntity().getProperties()) {
                            EdmProperty edmProperty = type.getEdmStructuredType().getStructuralProperty(property.getName());
                            writeProperty(metadata, edmProperty, property, null, json, null, type.getEntity(), null);
                        }
                        if (isCross) {
                            json.writeEndObject();
                        }
                    }
                  }
                }
                json.writeEndObject();
            }
            json.writeEndArray();

            if (nextLink != null) {
                json.writeStringField(Constants.JSON_NEXT_LINK, nextLink.toASCIIString());
            }

            json.close();
        } catch (final IOException | DecoderException e) {
            throw new SerializerException("An I/O exception occurred.", e, SerializerException.MessageKeys.IO_EXCEPTION);
        }
        return SerializerResultImpl.with().content(buffer.getInputStream()).build();
    }

    public SerializerResult complexCollection(final ServiceMetadata metadata,
            final EdmComplexType type,
            final Property result,
            final ContextURL contextURL, final URI nextLink) throws SerializerException {
        CircleStreamBuffer buffer = new CircleStreamBuffer();
        try {
            JsonGenerator json = new JsonFactory().createGenerator(buffer.getOutputStream());
            json.writeStartObject();

            if (contextURL != null && (isODataMetadataFull || !isODataMetadataNone)) {
                json.writeStringField(Constants.JSON_CONTEXT, ContextURLBuilder.create(contextURL).toASCIIString());
            }
            json.writeFieldName(Constants.VALUE);
            json.writeStartArray();
            for (Object value:result.asCollection()) {
                json.writeStartObject();
                writeComplexValue(metadata, type, ((ComplexValue) value).getValue(),
                        null, json, null, (ComplexValue) value, null, result.getName());
                json.writeEndObject();
            }
            json.writeEndArray();

            if (nextLink != null) {
                json.writeStringField(Constants.JSON_NEXT_LINK, nextLink.toASCIIString());
            }

            json.close();
        } catch (final IOException e) {
            throw new SerializerException("An I/O exception occurred.", e, SerializerException.MessageKeys.IO_EXCEPTION);
        }
        return SerializerResultImpl.with().content(buffer.getInputStream()).build();
    }
}
