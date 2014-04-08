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

package org.teiid.resource.adpter.simpledb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.resource.cci.Connection;

import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;


public interface SimpleDBConnection extends Connection{
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
     * @param itemName
     * @param columnsMap
     * @return
     */
    
    public int performInsert(String domainName, List<Column> columns, Iterator<? extends List<?>> values) throws TranslatorException;
    
    /**
     * Performs select expression. This expression must be in format which is understandable to SimpleDB database
     * @param selectExpression
     * @param columns
     * @return Iterator of List<String> results 
     */
    
    public com.amazonaws.services.simpledb.model.SelectResult performSelect(String selectExpression, String nextToken) throws TranslatorException;
    
    /**
     *  Performs update on given domain and items
     * @param domainName
     * @param items
     */
    
    public int performUpdate(String domainName, Map<String, Object> updateAttributes, String selectExpression) throws TranslatorException;
    
    /**
     * Removes item with given ItemName from domain
     * @param domainName
     * @param itemName
     */
    
    public int performDelete(String domainName, String selectExpression) throws TranslatorException;    
}
