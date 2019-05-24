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

import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.core.RequestURLHierarchyVisitor;

public class ExpandSQLBuilder extends RequestURLHierarchyVisitor {

    EdmNavigationProperty navProperty;

    public ExpandSQLBuilder(ExpandItem ei) {
        if (ei.getResourcePath() != null) {
            visit(ei.getResourcePath());
        }
    }

    public EdmNavigationProperty getNavigationProperty() {
        return this.navProperty;
    }

    @Override
    public void visit(UriResourceNavigation info) {
        this.navProperty = info.getProperty();
    }
}
