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
