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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.Document;

public class SwaggerResponse {
    
    private Iterator<Document> results;
    private List<Map<String, Object>> currentDocumentRows;
    private Map<String, Object> headers;
    private boolean mapResponse;
    
    public SwaggerResponse(InputStream payload, Map<String, Object> headers,
            SwaggerSerializer serializer, boolean mapResponse) throws TranslatorException {
        this.headers = headers;
        this.mapResponse = mapResponse;
        this.results = serializer.deserialize(payload).iterator();
    }

    public Map<String, Object> getNext() throws TranslatorException {
        
        if (this.currentDocumentRows != null && !this.currentDocumentRows.isEmpty()) {
            return this.currentDocumentRows.remove(0);
        }
        
        if (this.results.hasNext()) {
            Document d = this.results.next();
            List<Map<String, Object>> rows  = d.flatten();
            List<Map<String, Object>> mapResults = new ArrayList<Map<String, Object>>();
            if (this.mapResponse && !rows.isEmpty()) {
                for (String key:rows.get(0).keySet()) {
                    Map<String, Object> map = new LinkedHashMap<String, Object>();
                    map.put(key, rows.get(0).get(key));
                    mapResults.add(map);
                }
                rows = mapResults;
            }
            this.currentDocumentRows = rows;
            return getNext();
        } 
        return null;
    }
    
    public boolean isMapResponse() {
        return this.mapResponse;
    }
    
    public Map<String, Object> getHeaders(){
        return this.headers;
    }
}
