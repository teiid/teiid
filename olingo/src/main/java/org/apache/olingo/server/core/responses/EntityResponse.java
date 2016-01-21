/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.olingo.server.core.responses;

import java.util.Map;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.apache.olingo.commons.core.edm.primitivetype.EdmString;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.core.ContentNegotiatorException;
import org.apache.olingo.server.core.ReturnRepresentation;
import org.apache.olingo.server.core.ServiceRequest;
import org.apache.olingo.server.core.responses.ServiceResponse;
import org.apache.olingo.server.core.responses.ServiceResponseVisior;

/**
 * REMOVE AFTER Olingo 4.2.0 Update. 
 * has changes from https://issues.apache.org/jira/browse/OLINGO-854
 * DO NOT EDIT unless the changes are commited upstream
 */
public class EntityResponse extends ServiceResponse {
  private final ReturnRepresentation returnRepresentation;
  private final ODataSerializer serializer;
  private final EntitySerializerOptions options;
  private final ContentType responseContentType;
  private final String baseURL;

  private EntityResponse(ServiceMetadata metadata, ODataResponse response,
      ODataSerializer serializer, EntitySerializerOptions options, ContentType responseContentType,
      Map<String, String> preferences, ReturnRepresentation returnRepresentation, String baseURL) {
    super(metadata, response, preferences);
    this.serializer = serializer;
    this.options = options;
    this.responseContentType = responseContentType;
    this.returnRepresentation = returnRepresentation;
    this.baseURL = baseURL;
  }

  public static EntityResponse getInstance(ServiceRequest request, ContextURL contextURL,
      boolean references, ODataResponse response, ReturnRepresentation returnRepresentation)
      throws ContentNegotiatorException, SerializerException {
    EntitySerializerOptions options = request.getSerializerOptions(EntitySerializerOptions.class,
        contextURL, references);
    return new EntityResponse(request.getServiceMetaData(), response, request.getSerializer(),
        options, request.getResponseContentType(), request.getPreferences(), returnRepresentation, 
        request.getODataRequest().getRawBaseUri());
  }

  public static EntityResponse getInstance(ServiceRequest request, ContextURL contextURL,
      boolean references, ODataResponse response)
      throws ContentNegotiatorException, SerializerException {
    EntitySerializerOptions options = request.getSerializerOptions(EntitySerializerOptions.class,
        contextURL, references);
    return new EntityResponse(request.getServiceMetaData(), response, request.getSerializer(),
        options, request.getResponseContentType(), request.getPreferences(), null,
        request.getODataRequest().getRawBaseUri());
  }

  // write single entity
  public void writeReadEntity(EdmEntityType entityType, Entity entity) throws SerializerException {

    assert (!isClosed());

    if (entity == null) {
      writeNotFound(true);
      return;
    }

    // write the entity to response
    this.response.setContent(this.serializer.entity(this.metadata, entityType, entity, this.options).getContent());
    writeOK(responseContentType);
    close();
  }

  public void writeCreatedEntity(EdmEntitySet entitySet, Entity entity)
      throws SerializerException {
    // upsert/insert must created a entity, otherwise should have throw an
    // exception
    assert (entity != null);
    
    String locationHeader;
    try {
      locationHeader = buildLocation(this.baseURL, entity, entitySet.getName(), entitySet.getEntityType());
    } catch (EdmPrimitiveTypeException e) {
      throw new SerializerException(e.getMessage(), e, SerializerException.MessageKeys.WRONG_PRIMITIVE_VALUE);
    }

    // Note that if media written just like Stream, but on entity URL

    // 8.2.8.7
    if (this.returnRepresentation == ReturnRepresentation.MINIMAL || 
        this.returnRepresentation == ReturnRepresentation.NONE) {
      writeNoContent(false);
      writeHeader(HttpHeader.LOCATION, locationHeader);
      if (this.returnRepresentation == ReturnRepresentation.MINIMAL) {
        writeHeader("Preference-Applied", "return=minimal"); //$NON-NLS-1$ //$NON-NLS-2$
      }
      // 8.3.3
      writeHeader("OData-EntityId", entity.getId().toASCIIString()); //$NON-NLS-1$
      close();
      return;
    }

    // return the content of the created entity
    this.response.setContent(this.serializer.entity(this.metadata, entitySet.getEntityType(), entity, this.options)
        .getContent());
    writeCreated(false);
    writeHeader(HttpHeader.LOCATION, locationHeader);
    writeHeader("Preference-Applied", "return=representation"); //$NON-NLS-1$ //$NON-NLS-2$
    writeHeader(HttpHeader.CONTENT_TYPE, this.responseContentType.toContentTypeString());
    close();
  }

  public void writeUpdatedEntity() {
    // spec says just success response; so either 200 or 204. 200 typically has
    // payload
    writeNoContent(true);
  }

  public void writeDeletedEntityOrReference() {
    writeNoContent(true);
  }

  @Override
  public void accepts(ServiceResponseVisior visitor) throws ODataLibraryException,
      ODataApplicationException {
    visitor.visit(this);
  }

  public void writeCreated(boolean closeResponse) {
    this.response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
    if (closeResponse) {
      close();
    }
  }
  
  public void writeError(ODataServerError error) {
    try {
      writeContent(this.serializer.error(error).getContent(), error.getStatusCode(), true);
    } catch (SerializerException e) {
      writeServerError(true);
    }
  }
  
  public void writeNotModified() {
    this.response.setStatusCode(HttpStatusCode.NOT_MODIFIED.getStatusCode());
    close();
  }  
  
  public static String buildLocation(String baseURL, Entity entity, String enitySetName, EdmEntityType type) 
          throws EdmPrimitiveTypeException {
        StringBuilder location = new StringBuilder();
        location.append(baseURL).append("/").append(enitySetName);
        int i = 0;
        boolean usename = type.getKeyPredicateNames().size() > 1;
        location.append("(");
        for (String key : type.getKeyPredicateNames()) {
          if (i > 0) {
            location.append(",");
          }
          i++;
          if (usename) {
            location.append(key).append("=");
          }
          String propertyType = entity.getProperty(key).getType();
          Object propertyValue = entity.getProperty(key).getValue();
          
          if(propertyType.startsWith("Edm.")) {
            propertyType = propertyType.substring(4);
          }
          EdmPrimitiveTypeKind kind = EdmPrimitiveTypeKind.valueOf(propertyType);
          String value =  EdmPrimitiveTypeFactory.getInstance(kind).valueToString(
              propertyValue, true, 4000, 0, 0, true);
          if (kind == EdmPrimitiveTypeKind.String) {
              value = EdmString.getInstance().toUriLiteral(value);
          }
          location.append(value);
        }
        location.append(")");
        return location.toString();
      }  
}
