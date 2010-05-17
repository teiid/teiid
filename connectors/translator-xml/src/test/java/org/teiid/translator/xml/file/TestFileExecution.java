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

package org.teiid.translator.xml.file;

import java.io.File;
import java.sql.SQLXML;
import java.util.List;

import javax.resource.ResourceException;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.language.LanguageFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.xml.XMLExecutionFactory;
import org.teiid.translator.xml.file.TestCachingFileConnectorLong.FileImpl;


/** 
 */
@SuppressWarnings("nls")
public class TestFileExecution extends TestCase {
    
	public void testGoodFile() throws Exception {
		
		
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {				
				return new FileImpl(UnitTestUtil.getTestDataPath());
			}
			
		};
				
        RuntimeMetadata metadata = Mockito.mock(RuntimeMetadata.class);

        LanguageFactory fact = factory.getLanguageFactory();
       	Call procedure = fact.createCall("GetXMLFile", null, createMockProcedureMetadata("BookCollection.xml")); //$NON-NLS-1$

       	ProcedureExecution exec = factory.createProcedureExecution(procedure, Mockito.mock(ExecutionContext.class), metadata, cf); //$NON-NLS-1$ //$NON-NLS-2$
        
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
                    
        String fileContents = ObjectConverterUtil.convertFileToString(new File(UnitTestUtil.getTestDataPath()+"/BookCollection.xml")); //$NON-NLS-1$
        fileContents = fileContents.replaceAll("\r", ""); //$NON-NLS-1$ //$NON-NLS-2$
        //System.out.println(fileContents);
        
        assertEquals(fileContents, xml);
    }
        
    public void testBadFile() throws Exception {
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {				
				return new FileImpl(UnitTestUtil.getTestDataPath());
			}
			
		};
				
        RuntimeMetadata metadata = Mockito.mock(RuntimeMetadata.class);

        LanguageFactory fact = factory.getLanguageFactory();
       	Call procedure = fact.createCall("GetXMLFile", null, createMockProcedureMetadata("nofile.xml")); //$NON-NLS-1$

       	ProcedureExecution exec = factory.createProcedureExecution(procedure, Mockito.mock(ExecutionContext.class), metadata, cf); //$NON-NLS-1$ //$NON-NLS-2$

        try {
        	exec.execute();
            fail("should have thrown error in returning a return"); //$NON-NLS-1$
        }catch(Exception e) {                
        }        
    }    
 
    public static Procedure createMockProcedureMetadata(String nameInSource) {
    	Procedure rm = Mockito.mock(Procedure.class);
		Mockito.stub(rm.getNameInSource()).toReturn(nameInSource);
    	return rm;
    }
}
