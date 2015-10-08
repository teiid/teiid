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

import java.util.List;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.teiid.core.TeiidException;
import org.teiid.metadata.MetadataStore;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;


public class CrossJoinNode extends DocumentNode {
    private boolean expand;

    public static CrossJoinNode buildCrossJoin(EdmEntityType type,
            List<UriParameter> keyPredicates, MetadataStore metadata,
            UniqueNameGenerator nameGenerator, boolean useAlias,
            UriInfo uriInfo, URLParseService parseService, boolean expand) throws TeiidException {
        CrossJoinNode resource = new CrossJoinNode();
        build(resource, type, null, metadata, nameGenerator, useAlias, uriInfo, parseService);
        resource.setExpand(expand);
        return resource;
    }
    
    public boolean hasExpand() {
        return expand;
    }

    public void setExpand(boolean expand) {
        this.expand = expand;
    }
}
