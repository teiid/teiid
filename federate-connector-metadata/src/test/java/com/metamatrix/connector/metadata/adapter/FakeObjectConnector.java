/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.connector.sysadmin.extension.ISysAdminSource;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;

public class FakeObjectConnector extends ObjectConnector {
        private SimpleFakeObjectSource fakeObjectSource;
        
        public FakeObjectConnector(List objects) {
             fakeObjectSource = new SimpleFakeObjectSource(objects);
        }

        public String getTableNameQueried() {
            return fakeObjectSource.tableName;
        }
        
        
        /** 
         * @see com.metamatrix.connector.metadata.adapter.ObjectConnector#getMetadataObjectSource(com.metamatrix.data.api.SecurityContext)
         * @since 4.3
         */
        protected IObjectSource getMetadataObjectSource(SecurityContext context) throws ConnectorException {
            return fakeObjectSource;
        }

        /** 
         * @see com.metamatrix.connector.metadata.adapter.ObjectConnector#getSysAdminObjectSource(com.metamatrix.data.api.SecurityContext)
         * @since 4.3
         */
        protected ISysAdminSource getSysAdminObjectSource(SecurityContext context) throws ConnectorException {
            return null;
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
