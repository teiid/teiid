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
package org.teiid.translator.odata4;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.olingo.client.api.serialization.ODataDeserializerException;
import org.apache.olingo.client.core.serialization.JsonDeserializer;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.DocumentNode;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;


public abstract class ODataResponse {
    private URI nextUri;
    private Iterator<ODataDocument> results;
    private ODataType resultsType;
    private List<Map<String, Object>> currentDocumentRows;
    private DocumentNode rootNode;

    public ODataResponse(InputStream payload, ODataType type, DocumentNode rootNode) throws TranslatorException {
        this.resultsType = type;
        this.rootNode = rootNode;
        this.results = parsePayload(payload);
    }

    private Iterator<ODataDocument> parsePayload(InputStream payload) throws TranslatorException {
        try {
            JsonDeserializer parser = new JsonDeserializer(false);
            if (this.resultsType == ODataType.ENTITY) {
                Entity entity = parser.toEntity(payload).getPayload();
                ODataDocument document = ODataDocument.createDocument(entity);
                return Arrays.asList(document).iterator();
            } else if (this.resultsType == ODataType.ENTITY_COLLECTION) {
                EntityCollection entityCollection = parser.toEntitySet(payload).getPayload();
                this.nextUri = entityCollection.getNext();
                ArrayList<ODataDocument> documents = new ArrayList<ODataDocument>();
                for (Entity entity : entityCollection.getEntities()) {
                    documents.add(ODataDocument.createDocument(entity));
                }
                return documents.iterator();
            } else {
                // complex
                Property property = parser.toProperty(payload).getPayload();
                if (property.isCollection()) {
                    ArrayList<ODataDocument> documents = new ArrayList<ODataDocument>();
                    for (Object obj : property.asCollection()) {
                        ComplexValue complexValue = (ComplexValue)obj;
                        documents.add(ODataDocument.createDocument(complexValue));
                    }
                    return documents.iterator();
                } else {
                    ODataDocument document = ODataDocument.createDocument(property.asComplex());
                    return Arrays.asList(document).iterator();
                }
            }
        } catch (ODataDeserializerException e) {
            throw new TranslatorException(e);
        }
    }

    public Map<String, Object> getNext() throws TranslatorException {

        if (this.currentDocumentRows != null && !this.currentDocumentRows.isEmpty()) {
            return this.currentDocumentRows.remove(0);
        }

        if (this.results.hasNext()) {
            this.currentDocumentRows = this.rootNode.tuples(this.results.next());
            return getNext();
        } else {
            if (this.nextUri != null) {
                this.results = fetchSkipToken(this.nextUri);
                return getNext();
            }
        }
        return null;
    }

    private Iterator<ODataDocument> fetchSkipToken(URI uri) throws TranslatorException {
        return parsePayload(nextBatch(uri));
    }

    public abstract InputStream nextBatch(URI uri) throws TranslatorException;
}

