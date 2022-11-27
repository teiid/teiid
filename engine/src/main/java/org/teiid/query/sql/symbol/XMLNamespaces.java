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

package org.teiid.query.sql.symbol;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


public class XMLNamespaces implements LanguageObject {

    private static final long serialVersionUID = 681076404921001047L;

    public static class NamespaceItem {
        private String uri;
        private String prefix;

        public NamespaceItem(String uri, String prefix) {
            this.uri = uri;
            this.prefix = prefix;
        }

        public NamespaceItem(String defaultNamepace) {
            this.uri = defaultNamepace;
        }

        public NamespaceItem() {
        }

        public String getUri() {
            return uri;
        }

        public String getPrefix() {
            return prefix;
        }

        @Override
        public int hashCode() {
            return HashCodeUtil.hashCode(0, this.uri, this.prefix);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof NamespaceItem)) {
                return false;
            }
            NamespaceItem other = (NamespaceItem)obj;
            return EquivalenceUtil.areEqual(this.uri, other.uri) &&
                EquivalenceUtil.areEqual(this.prefix, other.prefix);
        }
    }

    private List<NamespaceItem> namespaceItems;


    public XMLNamespaces(List<NamespaceItem> namespaceItems) {
        this.namespaceItems = namespaceItems;
    }

    public List<NamespaceItem> getNamespaceItems() {
        return namespaceItems;
    }

    @Override
    public XMLNamespaces clone() {
        XMLNamespaces clone = new XMLNamespaces(new ArrayList<NamespaceItem>(namespaceItems));
        return clone;
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(namespaceItems.hashCode());
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof XMLNamespaces)) {
            return false;
        }
        XMLNamespaces other = (XMLNamespaces)obj;
        return namespaceItems.equals(other.namespaceItems);
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
