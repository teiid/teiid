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

package org.teiid.translator.simpledb.api;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.metadata.Column;
import org.teiid.resource.api.Connection;
import org.teiid.translator.TranslatorException;


public interface SimpleDBConnection extends Connection {
    public static final String ITEM_NAME = "itemName()"; //$NON-NLS-1$

    public static class SimpleDBAttribute {
        private String name;
        private boolean multi;

        public SimpleDBAttribute(String name, boolean multi) {
            this.name = name;
            this.multi = multi;
        }

        public String getName() {
            return this.name;
        }

        public boolean hasMultipleValues() {
            return this.multi;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SimpleDBAttribute other = (SimpleDBAttribute) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }
    }

    /**
     * Create a domain
     * @param domainName
     * @throws TranslatorException
     */
    public void createDomain(String domainName) throws TranslatorException;

    /**
     * Delete a Domain
     * @param domainName
     * @throws TranslatorException
     */
    public void deleteDomain(String domainName) throws TranslatorException;

    /**
     * Lists all domains of database
     * @return
     */
    public List<String> getDomains() throws TranslatorException;

    /**
     * Get the attributes for given domain name
     * @param domainName
     * @return Set of attribute names for given domain
     */

    public Set<SimpleDBAttribute> getAttributeNames(String domainName) throws TranslatorException;

    /**
     *  Inserts item into given domain.
     * @param domainName
     * @return
     */

    public int performInsert(String domainName, List<Column> columns, Iterator<? extends List<?>> values) throws TranslatorException;

    /**
     * Performs select expression. This expression must be in format which is understandable to SimpleDB database
     * @param selectExpression
     */
    public com.amazonaws.services.simpledb.model.SelectResult performSelect(String selectExpression, String nextToken) throws TranslatorException;

    /**
     *  Performs update on given domain and items
     * @param domainName
     */

    public int performUpdate(String domainName, Map<String, Object> updateAttributes, String selectExpression) throws TranslatorException;

    /**
     * Removes item with given ItemName from domain
     * @param domainName
     */

    public int performDelete(String domainName, String selectExpression) throws TranslatorException;
}
