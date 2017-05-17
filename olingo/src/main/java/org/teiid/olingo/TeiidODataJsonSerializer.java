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

import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.core.serializer.SerializerResultImpl;
import org.apache.olingo.server.core.serializer.json.ODataJsonSerializer;
import org.apache.olingo.server.core.serializer.utils.CircleStreamBuffer;
import org.apache.olingo.server.core.serializer.utils.ContextURLBuilder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class TeiidODataJsonSerializer extends ODataJsonSerializer {
    
    public TeiidODataJsonSerializer(ContentType contentType) {
        super(contentType);
    }

    public SerializerResult complexCollection(final ServiceMetadata metadata,
            final List<List<ComplexReturnType>> result,
            final ContextURL contextURL, final URI nextLink) throws SerializerException {
        CircleStreamBuffer buffer = new CircleStreamBuffer();
        try {
            JsonGenerator json = new JsonFactory().createGenerator(buffer.getOutputStream());
            json.writeStartObject();

            if (contextURL != null) {
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
                    writeEntity(metadata, type.getEdmEntityType(), type.getEntity(), null, null, null, null, false, null, json);
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
        }
        return SerializerResultImpl.with().content(buffer.getInputStream()).build();
    }    
}
