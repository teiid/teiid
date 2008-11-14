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

package com.metamatrix.connector.xmlsource.file;

import java.io.FileReader;
import java.io.Reader;
import java.sql.SQLXML;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.xmlsource.FakeRuntimeMetadata;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ILanguageFactory;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.visitor.framework.LanguageObjectVisitor;


/** 
 */
public class TestFileExecution extends TestCase {
    
    public void testGoodFile() throws Exception {
        String file = UnitTestUtil.getTestDataPath(); 
        Properties props = new Properties();
        props.setProperty("DirectoryLocation", file); //$NON-NLS-1$  
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        try {
            FileConnection conn = new FileConnection(env);
            assertTrue(conn.isConnected());
            RuntimeMetadata metadata = new FakeRuntimeMetadata("BookCollection.xml"); //$NON-NLS-1$
            FileExecution exec = (FileExecution)conn.createExecution(2, EnvironmentUtility.createExecutionContext("100", "100"), metadata); //$NON-NLS-1$ //$NON-NLS-2$
            
            ILanguageFactory fact = env.getLanguageFactory();
            IProcedure procedure = fact.createProcedure("GetXMLFile", null, null); //$NON-NLS-1$
            
            exec.execute(procedure, 100);
            
            Batch b = exec.nextBatch();
            assertNotNull(b);
            assertTrue(b.isLast());
            List[] lists = b.getResults();
            assertEquals("There should be only one row", 1, b.getRowCount()); //$NON-NLS-1$
            try {
                exec.getOutputValue(getReturnParameter());
                fail("should have thrown error in returning a return"); //$NON-NLS-1$
            }catch(Exception e) {                
            }
            SQLXML xmlSource = (SQLXML)lists[0].get(0);            
            assertNotNull(xmlSource);
            String xml = xmlSource.getString();
                        
            String fileContents = readFile(file+"/BookCollection.xml"); //$NON-NLS-1$
            fileContents = fileContents.replaceAll("\r", ""); //$NON-NLS-1$ //$NON-NLS-2$
            //System.out.println(fileContents);
            
            assertEquals(fileContents, xml);
        } catch (ConnectorException e) {
            e.printStackTrace();
            fail("must have passed connection"); //$NON-NLS-1$
        }                    
    }
        
    public void testBadFile() {
        String file = UnitTestUtil.getTestDataPath(); 
        Properties props = new Properties();
        props.setProperty("DirectoryLocation", file); //$NON-NLS-1$  
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        try {
            FileConnection conn = new FileConnection(env);
            assertTrue(conn.isConnected());
            RuntimeMetadata metadata = new FakeRuntimeMetadata("nofile.xml"); //$NON-NLS-1$
            FileExecution exec = (FileExecution)conn.createExecution(2, EnvironmentUtility.createExecutionContext("100", "100"), metadata); //$NON-NLS-1$ //$NON-NLS-2$
            
            ILanguageFactory fact = env.getLanguageFactory();
            IProcedure procedure = fact.createProcedure("GetXMLFile", null, null); //$NON-NLS-1$
            
            exec.execute(procedure, 100);
            fail("mast have failed to find the file"); //$NON-NLS-1$            
        } catch (ConnectorException e) {
            //pass
        }         
    }    
 
    
    IParameter getReturnParameter() {
        return new IParameter() {
            public int getIndex() {
                return 0;
            }
            public int getDirection() {
                return IParameter.RETURN;
            }
            public Class getType() {
                return null;
            }
            public Object getValue() {
                return null;
            }
            public boolean getValueSpecified() {
                return false;
            }
            public void setIndex(int index) {}
            public void setDirection(int direction) {}
            public void setType(Class type) {}
            public void setValue(Object value) {}
            public void setValueSpecified(boolean specified) {}
            public void acceptVisitor(LanguageObjectVisitor visitor) {}
            public MetadataID getMetadataID() {
                return null;
            }
            public void setMetadataID(MetadataID metadataID) {}            
        };     
    }      
    
    String readFile(String filename) throws Exception {
        Reader reader = new FileReader(filename); 
        StringBuffer fileContents = new StringBuffer();
        int c= reader.read();
        while (c != -1) {
            fileContents.append((char)c);
            c = reader.read();
        }        
        reader.close();
        return fileContents.toString();
    }    
}
