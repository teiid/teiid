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

package com.metamatrix.connector.xmlsource.file;

import java.io.FileReader;
import java.io.Reader;
import java.sql.SQLXML;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.Procedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;


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
            RuntimeMetadata metadata = Mockito.mock(RuntimeMetadata.class);

            ILanguageFactory fact = env.getLanguageFactory();
           	IProcedure procedure = fact.createProcedure("GetXMLFile", null, createMockProcedureMetadata("BookCollection.xml")); //$NON-NLS-1$

            FileExecution exec = (FileExecution)conn.createExecution(procedure, EnvironmentUtility.createExecutionContext("100", "100"), metadata); //$NON-NLS-1$ //$NON-NLS-2$
            
            exec.execute();
            
            List result = exec.next();
            assertNotNull(result);
            assertNull(exec.next());
            try {
                exec.getOutputParameterValues();
                fail("should have thrown error in returning a return"); //$NON-NLS-1$
            }catch(Exception e) {                
            }
            SQLXML xmlSource = (SQLXML)result.get(0);            
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
            RuntimeMetadata metadata = Mockito.mock(RuntimeMetadata.class);
            ILanguageFactory fact = env.getLanguageFactory();
            FileExecution exec = (FileExecution)conn.createExecution(fact.createProcedure("GetXMLFile", null, createMockProcedureMetadata("nofile.xml")), EnvironmentUtility.createExecutionContext("100", "100"), metadata); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            
            
            exec.execute();
            fail("mast have failed to find the file"); //$NON-NLS-1$            
        } catch (ConnectorException e) {
            //pass
        }         
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
    
    public static Procedure createMockProcedureMetadata(String nameInSource) {
    	Procedure rm = Mockito.mock(Procedure.class);
    	try {
			Mockito.stub(rm.getNameInSource()).toReturn(nameInSource);
		} catch (ConnectorException e) {
			throw new RuntimeException(e);
		}
    	return rm;
    }
}
