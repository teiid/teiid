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

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmComplexType;
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
                json.writeStartObject();
                for (final ComplexReturnType type : ct) {
                  if (!type.isExpand()) {
                    json.writeStringField(type.getName()+Constants.JSON_NAVIGATION_LINK, type.getEntity().getId().toASCIIString());
                  } 
                  else {
                    json.writeFieldName(type.getName());                      
                    writeEntity(metadata, type.getEdmEntityType(), type.getEntity(), null, null, null, null, false, null, type.getName(), json);
                  }
                }
                json.writeEndObject();
            }
            json.writeEndArray();
            
            if (nextLink != null) {
                json.writeStringField(Constants.JSON_NEXT_LINK, nextLink.toASCIIString());
            }
            
            json.close();
        } catch (final IOException e) {
            throw new SerializerException("An I/O exception occurred.", e, SerializerException.MessageKeys.IO_EXCEPTION);
        } catch (final DecoderException e) {
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
