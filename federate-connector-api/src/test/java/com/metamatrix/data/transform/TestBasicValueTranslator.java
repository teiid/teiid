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

package com.metamatrix.data.transform;

import java.io.Serializable;

import junit.framework.TestCase;

import com.metamatrix.connector.jdbc.extension.impl.BasicValueTranslator;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.exception.ConnectorException;


/** 
 * @since 4.2
 */
public class TestBasicValueTranslator extends TestCase {


    public void testExceptionFromTransform() throws Exception {
    	BasicValueTranslator adaptor = new BasicValueTranslator(String.class, Short.class, new FakeTypeFacility());
        try {
            adaptor.translate("mmuuid:blah", new FakeExecutionContextImpl()); //$NON-NLS-1$
        } catch(ConnectorException e) {
            // expected
        }
    }
    
    static class FakeTypeFacility extends TypeFacility{

		@Override
		public boolean hasTransformation(Class sourceClass, Class targetClass) {
			return false;
		}

		@Override
		public Object transformValue(Object value, Class sourceClass, Class targetClass) throws ConnectorException {
			throw new ConnectorException("not supported");
		}
		
		@Override
		public Object convertToRuntimeType(Object value) {
			return value;
		}
    }
    
    public class FakeExecutionContextImpl implements com.metamatrix.data.api.ExecutionContext {

		public String getConnectionIdentifier() {
			return null;
		}
		public String getConnectorIdentifier() {
			return null;
		}
		public String getExecutionCountIdentifier() {
			return null;
		}
		public Serializable getExecutionPayload() {
			return null;
		}
		public String getPartIdentifier() {
			return null;
		}
		public String getRequestIdentifier() {
			return null;
		}
		public Serializable getTrustedPayload() {
			return null;
		}
		public String getUser() {
			return null;
		}
		public String getVirtualDatabaseName() {
			return null;
		}
		public String getVirtualDatabaseVersion() {
			return null;
		}
		public void keepExecutionAlive(boolean alive) {
			
		}
		public boolean useResultSetCache() {
			return false;
		}
        
    }
    
}
