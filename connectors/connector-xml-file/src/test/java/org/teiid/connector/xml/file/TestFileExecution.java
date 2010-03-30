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

package org.teiid.connector.xml.file;

import java.io.File;
import java.io.PrintWriter;
import java.sql.SQLXML;
import java.util.List;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.LanguageFactory;
import org.teiid.connector.metadata.runtime.Procedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.xml.file.FileConnection;
import org.teiid.connector.xml.file.FileManagedConnectionFactory;
import org.teiid.connector.xml.file.FileProcedureExecution;

import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 */
@SuppressWarnings("nls")
public class TestFileExecution extends TestCase {
    
	public void testGoodFile() throws Exception {
        String file = UnitTestUtil.getTestDataPath(); 
        FileManagedConnectionFactory config = new FileManagedConnectionFactory();
        config.setLogWriter(Mockito.mock(PrintWriter.class));
        config.setDirectoryLocation(file);
        
        FileConnection conn = new FileConnection(config);
        assertTrue(conn.isConnected());
        RuntimeMetadata metadata = Mockito.mock(RuntimeMetadata.class);

        LanguageFactory fact = config.getLanguageFactory();
       	Call procedure = fact.createCall("GetXMLFile", null, createMockProcedureMetadata("BookCollection.xml")); //$NON-NLS-1$

        FileProcedureExecution exec = (FileProcedureExecution)conn.createExecution(procedure, Mockito.mock(ExecutionContext.class), metadata); //$NON-NLS-1$ //$NON-NLS-2$
        
        exec.execute();
        
        List<?> result = exec.next();
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
                    
        String fileContents = ObjectConverterUtil.convertFileToString(new File(file+"/BookCollection.xml")); //$NON-NLS-1$
        fileContents = fileContents.replaceAll("\r", ""); //$NON-NLS-1$ //$NON-NLS-2$
        //System.out.println(fileContents);
        
        assertEquals(fileContents, xml);
    }
        
    public void testBadFile() throws Exception {
        String file = UnitTestUtil.getTestDataPath(); 
        FileManagedConnectionFactory config = new FileManagedConnectionFactory();
        config.setLogWriter(Mockito.mock(PrintWriter.class));
        config.setDirectoryLocation(file);

        
        try {
            FileConnection conn = new FileConnection(config);
            assertTrue(conn.isConnected());
            RuntimeMetadata metadata = Mockito.mock(RuntimeMetadata.class);
            LanguageFactory fact = config.getLanguageFactory();
            FileProcedureExecution exec = (FileProcedureExecution)conn.createExecution(fact.createCall("GetXMLFile", null, createMockProcedureMetadata("nofile.xml")), Mockito.mock(ExecutionContext.class), metadata); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            
            
            exec.execute();
            fail("mast have failed to find the file"); //$NON-NLS-1$            
        } catch (ConnectorException e) {
            //pass
        }         
    }    
 
    public static Procedure createMockProcedureMetadata(String nameInSource) {
    	Procedure rm = Mockito.mock(Procedure.class);
		Mockito.stub(rm.getNameInSource()).toReturn(nameInSource);
    	return rm;
    }
}
