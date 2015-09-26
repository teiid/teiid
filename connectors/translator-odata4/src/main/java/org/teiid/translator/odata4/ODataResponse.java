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
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;


public abstract class ODataResponse {
    private URI nextUri;
    private Iterator<ODataResponseDocument> results;
    private ODataType resultsType;
    private List<Map<String, Object>> currentDocumentRows;
    
    public ODataResponse(InputStream payload, ODataType type) throws TranslatorException {
        this.resultsType = type;
        this.results = parsePayload(payload);        
    }

    private Iterator<ODataResponseDocument> parsePayload(InputStream payload) throws TranslatorException {
        try {
            JsonDeserializer parser = new JsonDeserializer(false);
            if (this.resultsType == ODataType.ENTITY) {
                Entity entity = parser.toEntity(payload).getPayload();
                ODataResponseDocument document = ODataResponseDocument.createDocument(entity);
                return Arrays.asList(document).iterator();
            } else if (this.resultsType == ODataType.ENTITY_COLLECTION) {
                EntityCollection entityCollection = parser.toEntitySet(payload).getPayload();
                this.nextUri = entityCollection.getNext();
                ArrayList<ODataResponseDocument> documents = new ArrayList<ODataResponseDocument>();
                for (Entity entity : entityCollection.getEntities()) {
                    documents.add(ODataResponseDocument.createDocument(entity));
                }
                return documents.iterator();            
            } else {
                // complex
                Property property = parser.toProperty(payload).getPayload();
                if (property.isCollection()) {
                    ArrayList<ODataResponseDocument> documents = new ArrayList<ODataResponseDocument>();
                    for (Object obj : property.asCollection()) {
                        ComplexValue complexValue = (ComplexValue)obj;
                        documents.add(ODataResponseDocument.createDocument(complexValue));
                    }
                    return documents.iterator();
                } else {
                    ODataResponseDocument document = ODataResponseDocument.createDocument(property.asComplex());
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
            this.currentDocumentRows = this.results.next().flatten();
            return getNext();
        } else {
            if (this.nextUri != null) {
                this.results = fetchSkipToken(this.nextUri);
                return getNext();
            }            
        }
        return null;
    }

    private Iterator<ODataResponseDocument> fetchSkipToken(URI uri) throws TranslatorException {
        return parsePayload(nextBatch(uri));
    }
    
    public abstract InputStream nextBatch(URI uri) throws TranslatorException;
}

