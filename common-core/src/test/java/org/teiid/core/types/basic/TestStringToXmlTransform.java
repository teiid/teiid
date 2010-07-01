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

package org.teiid.core.types.basic;

import static org.junit.Assert.*;

import java.sql.SQLXML;

import org.junit.Test;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.basic.StringToSQLXMLTransform;


@SuppressWarnings("nls")
public class TestStringToXmlTransform {

	@Test public void testGoodXML() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><customer>\n" + //$NON-NLS-1$
                        "<name>ABC</name>" + //$NON-NLS-1$
                        "<age>32</age>" + //$NON-NLS-1$
                     "</customer>"; //$NON-NLS-1$
        
       StringToSQLXMLTransform transform = new StringToSQLXMLTransform();
       
       SQLXML xmlValue = (SQLXML)transform.transformDirect(xml);
       assertEquals(xml.replaceAll("[\r]", ""), xmlValue.getString().replaceAll("[\r]", ""));
    }
    
    @Test(expected=TransformationException.class) public void testBadXML() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><customer>\n" + //$NON-NLS-1$
                        "<name>ABC</name>" + //$NON-NLS-1$
                        "<age>32</age>" + //$NON-NLS-1$
                     "<customer>"; //$NON-NLS-1$ (********no ending)
        
       StringToSQLXMLTransform transform = new StringToSQLXMLTransform();
       
       transform.transformDirect(xml);
    }    
    
}
