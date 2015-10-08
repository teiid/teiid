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
package org.teiid.olingo.service;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.server.api.uri.UriInfo;
import org.teiid.core.TeiidException;
import org.teiid.metadata.MetadataStore;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;


public class ExpandDocumentNode extends DocumentNode {
    private String navigationName;
    private boolean collection;
    
    public static ExpandDocumentNode buildExpand(EdmNavigationProperty property,
            MetadataStore metadata, UniqueNameGenerator nameGenerator,
            boolean useAlias, UriInfo uriInfo, URLParseService parseService) throws TeiidException {
        
        EdmEntityType type = property.getType();
        ExpandDocumentNode resource = new ExpandDocumentNode();
        build(resource, type, null, metadata, nameGenerator, useAlias, uriInfo, parseService);
        resource.setNavigationName(property.getName());
        resource.setCollection(property.isCollection());
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
}
