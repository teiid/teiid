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
package org.teiid.translator.swagger;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.swagger.SwaggerProcedureExecution.Deserializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonDeserializer implements Deserializer {

    private ObjectMapper mapper;
    
    private JsonFactory jsonFactory;
    
    public JsonDeserializer() {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
        mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
        jsonFactory = new JsonFactory(mapper);
      }
    
    public Object deserialize(InputStream input) throws TranslatorException {
        try {
            JsonParser parser = jsonFactory.createParser(input);
            return parser.getCodec().readValue(parser, Object.class);
        } catch (Exception e) {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28007, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28007, e.getMessage()));
        } 
    }
    
    public String toXmlString(InputStream input) throws TranslatorException {
        
        try {
            StringWriter output = new StringWriter();
            IOUtils.copy(input, output);
            return output.toString();
        } catch (IOException e) {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28007, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28007, e.getMessage()));
        }
    }
    
    public String toJsonString(InputStream input) throws TranslatorException {
        try {
            Object value = mapper.readValue(input, Object.class);
            return this.mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28007, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28007, e.getMessage()));
        }
    }
}
