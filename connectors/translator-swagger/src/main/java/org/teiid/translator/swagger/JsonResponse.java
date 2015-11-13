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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.JsonDocument;
import org.teiid.translator.swagger.SwaggerExecutionFactory.ResultsType;

public class JsonResponse {
    
    private Iterator<JsonDocument> results;
    private ResultsType resultsType;
    private DocumentNode rootNode;
    
    public JsonResponse(InputStream payload, ResultsType resultsType, DocumentNode rootNode) throws TranslatorException {
        this.resultsType = resultsType;
        this.rootNode = rootNode;
        this.results = parsePayload(payload);
    }

    @SuppressWarnings("unchecked")
    private Iterator<JsonDocument> parsePayload(InputStream payload) throws TranslatorException {
        
        try {
            JsonDeserializer parser = new JsonDeserializer();
            if(resultsType.equals(ResultsType.REF)){
                Map<String, Object> results = parser.deserialize(payload, Map.class);
                JsonDocument document = JsonDocument.createDocument(null, null, results);
                return Arrays.asList(document).iterator();
            } else if(resultsType.equals(ResultsType.ARRAY)){
                List<Map<String, Object>> results = parser.deserialize(payload, List.class);
                List<JsonDocument> list = new ArrayList<JsonDocument>();
                for(Map<String, Object> result : results){
                    JsonDocument document = JsonDocument.createDocument(null, null, result);
                    list.add(document);
                }
                return list.iterator();
            }
        } catch (Exception e) {
            throw new TranslatorException(e);
        }
        return null;
    }

    public Map<String, Object> getNext() {
        
        if (this.results.hasNext()) {
            JsonDocument document = results.next();
            return document.getProperties();
        }
        
        return null;
    }

}
