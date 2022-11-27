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
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.uri.UriInfo;
import org.teiid.core.TeiidException;
import org.teiid.metadata.MetadataStore;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;


public class CrossJoinNode extends DocumentNode {
    private boolean expand;

    public static CrossJoinNode buildCrossJoin(EdmEntityType type, MetadataStore metadata, OData odata,
            UniqueNameGenerator nameGenerator, boolean useAlias,
            UriInfo uriInfo, URLParseService parseService, boolean expand) throws TeiidException {
        CrossJoinNode resource = new CrossJoinNode();
        build(resource, type, null, metadata, odata, nameGenerator, useAlias, uriInfo, parseService);
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
