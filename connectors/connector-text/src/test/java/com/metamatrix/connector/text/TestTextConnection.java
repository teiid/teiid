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

package com.metamatrix.connector.text;

import java.util.Map;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.cci.text.TextConnectionFactory;
import org.teiid.resource.cci.text.TextConnectionImpl;
import org.teiid.resource.cci.text.TextManagedConnectionFactory;

import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestTextConnection extends TestCase {

    /**
     * @param name
     */
    public TestTextConnection(String name) {
        super(name);
    }
    
    public void testDefect10371() throws Exception {    
    	String descFile = UnitTestUtil.getTestDataPath() + "/EmployeeTestDataSalary.txt"; //$NON-NLS-1$
        TextManagedConnectionFactory config = Mockito.mock(TextManagedConnectionFactory.class);
        Mockito.stub(config.getDescriptorFile()).toReturn(descFile);
        
        TextConnectionFactory tcf = new TextConnectionFactory(config);
        TextConnectionImpl conn = (TextConnectionImpl)tcf.getConnection();
        
        Map actualProps = conn.getMetadataProperties();
        assertNotNull(actualProps);
    }

    /**
     * Test partial startup property - test default - should default to allow partial startup
     */
    public void testCase4284Default() throws Exception {        
        String descFile = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$
            
        TextManagedConnectionFactory config = Mockito.mock(TextManagedConnectionFactory.class);
        Mockito.stub(config.getDescriptorFile()).toReturn(descFile);
        Mockito.stub(config.isPartialStartupAllowed()).toReturn(true);
        
        TextConnectionFactory tcf = new TextConnectionFactory(config);
        assertNotNull(tcf.getConnection());
    }

    /**
     * Test partial startup property - disallow partial startup
     */
    public void testCase4284DisallowPartial() throws Exception {        
        String descFile = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$
            
        TextManagedConnectionFactory config = Mockito.mock(TextManagedConnectionFactory.class);
        Mockito.stub(config.getDescriptorFile()).toReturn(descFile);
        Mockito.stub(config.isPartialStartupAllowed()).toReturn(false);
        
        try {
        	new TextConnectionFactory(config);
        	fail("expected exception"); //$NON-NLS-1$
        } catch (ConnectorException e) {
            String m1 = "Error parsing property string text.library2.location"; //$NON-NLS-1$
            String m2 = "Data file not found at this location:"; //$NON-NLS-1$
            String m3 = UnitTestUtil.getTestDataPath() + "/libraryDelimited2.txt, for group TEXT.LIBRARY2"; //$NON-NLS-1$
            
        	String message = e.getMessage();
            if ( (message.indexOf(m1) == -1) || message.indexOf(m2) == -1 || message.indexOf(m3) == -1)
                fail("Error message doesnt match the expected message"); //$NON-NLS-1$
                //assertEquals("Error message doesnt match the expected message","Error parsing property string text.library2.location = testdata/libraryDelimited2.txt: Data file not found at this location: testdata/libraryDelimited2.txt, for group TEXT.LIBRARY2",message);
        }
    }

    /**
     * Test partial startup property - allow partial startup
     */
    public void testCase4284AllowPartial() throws Exception {        
        String descFile = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$
        
        TextManagedConnectionFactory config = Mockito.mock(TextManagedConnectionFactory.class);
        Mockito.stub(config.getDescriptorFile()).toReturn(descFile);
        Mockito.stub(config.isPartialStartupAllowed()).toReturn(true);
        
        TextConnectionFactory tcf = new TextConnectionFactory(config);
        assertNotNull(tcf.getConnection());
    }

}
