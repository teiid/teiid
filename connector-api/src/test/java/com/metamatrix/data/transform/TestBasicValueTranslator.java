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

package com.metamatrix.data.transform;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.basic.BasicValueTranslator;


/** 
 * @since 4.2
 */
public class TestBasicValueTranslator extends TestCase {

    public void testExceptionFromTransform() throws Exception {
    	BasicValueTranslator<String, Short> adaptor = BasicValueTranslator.createTranslator(String.class, Short.class, Mockito.mock(TypeFacility.class));
        try {
            adaptor.translate("mmuuid:blah", Mockito.mock(ExecutionContext.class)); //$NON-NLS-1$
            fail("exceptoin expected"); //$NON-NLS-1$
        } catch(ConnectorException e) {
            // expected
        }
    }
    
}
