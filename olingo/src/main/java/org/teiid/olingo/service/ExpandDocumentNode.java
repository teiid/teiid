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
package org.teiid.olingo.service;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.uri.UriInfo;
import org.teiid.core.TeiidException;
import org.teiid.metadata.MetadataStore;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;


public class ExpandDocumentNode extends DocumentNode {
    private String navigationName;
    private boolean collection;
    private int top=-1;
    private int skip;
    private int columnIndex;
    private DocumentNode collectionContext;

    public static ExpandDocumentNode buildExpand(EdmNavigationProperty property,
            MetadataStore metadata, OData odata, UniqueNameGenerator nameGenerator,
            boolean useAlias, UriInfo uriInfo, URLParseService parseService, DocumentNode context) throws TeiidException {

        EdmEntityType type = property.getType();
        ExpandDocumentNode resource = new ExpandDocumentNode();
        build(resource, type, null, metadata, odata, nameGenerator, useAlias, uriInfo, parseService);
        resource.setNavigationName(property.getName());
        resource.setCollection(property.isCollection());
        resource.collectionContext = context;
        return resource;
    }

    public String getNavigationName() {
        return navigationName;
    }

    public void setNavigationName(String navigationName) {
        this.navigationName = navigationName;
    }

    public boolean isCollection() {
        return collection;
    }

    public void setCollection(boolean collection) {
        this.collection = collection;
    }

    public void setTop(int value) {
        this.top = value;
    }

    public int getTop() {
        return top;
    }

    public void setSkip(int value) {
        this.skip = value;
    }

    public int getSkip() {
        return skip;
    }

    public void setColumnIndex(int count) {
        this.columnIndex = count;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public DocumentNode getCollectionContext() {
        return collectionContext;
    }

}
