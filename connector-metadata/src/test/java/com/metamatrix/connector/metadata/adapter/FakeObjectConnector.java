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

package com.metamatrix.connector.metadata.adapter;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.connector.metadata.internal.IObjectSource;

public class FakeObjectConnector extends ObjectConnector {
        private SimpleFakeObjectSource fakeObjectSource;
        
        public FakeObjectConnector(List objects) {
             fakeObjectSource = new SimpleFakeObjectSource(objects);
        }

        public String getTableNameQueried() {
            return fakeObjectSource.tableName;
        }
        
        
        /** 
         * @see com.metamatrix.connector.metadata.adapter.ObjectConnector#getMetadataObjectSource(org.teiid.connector.api.ExecutionContext)
         * @since 4.3
         */
        protected IObjectSource getMetadataObjectSource(ExecutionContext context) throws ConnectorException {
            return fakeObjectSource;
        }

    }

    class SimpleFakeObjectSource implements IObjectSource {
        private List objects;
        public String tableName;

        public SimpleFakeObjectSource(List objects) {
            this.objects = objects;
        }

        /* 
         * @see com.metamatrix.connector.metadata.internal.IObjectSource#getObjects(java.lang.String, java.util.Map)
         */
        public Collection getObjects(String tableName, Map criteria) {
            this.tableName = tableName;
            return objects;
        }
    }
