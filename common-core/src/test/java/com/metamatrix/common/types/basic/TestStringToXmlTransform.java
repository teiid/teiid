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

package com.metamatrix.common.types.basic;

//## JDBC4.0-begin ##
import java.sql.SQLXML;
//## JDBC4.0-end ##

/*## JDBC3.0-JDK1.5-begin ##
import com.metamatrix.core.jdbc.SQLXML; 
## JDBC3.0-JDK1.5-end ##*/

import com.metamatrix.common.types.TransformationException;

import junit.framework.TestCase;


public class TestStringToXmlTransform extends TestCase {

    public void testGoodXML() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><customer>\n" + //$NON-NLS-1$
                        "<name>ABC</name>" + //$NON-NLS-1$
                        "<age>32</age>" + //$NON-NLS-1$
                     "</customer>"; //$NON-NLS-1$
        
       StringToSQLXMLTransform transform = new StringToSQLXMLTransform();
       
       SQLXML xmlValue = (SQLXML)transform.transform(xml);
       assertEquals(xml, xmlValue.getString());
    }
    
    public void testBadXML() {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><customer>\n" + //$NON-NLS-1$
                        "<name>ABC</name>" + //$NON-NLS-1$
                        "<age>32</age>" + //$NON-NLS-1$
                     "<customer>"; //$NON-NLS-1$ (********no ending)
        
       StringToSQLXMLTransform transform = new StringToSQLXMLTransform();
       
       try {
           transform.transform(xml);
           fail("exception expected"); //$NON-NLS-1$           
        } catch (TransformationException e) {        
        }        
    }    
}
