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

package com.metamatrix.query.processor.dynamic;

import java.util.Collections;

import javax.xml.transform.Source;

import junit.framework.TestCase;

import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.types.StandardXMLTranslator;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.FakeDataStore;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.processor.xml.TestXMLProcessor;
import com.metamatrix.query.processor.xquery.SqlEval;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;


/** 
 *This is test case to convert SQL results to XML during processor
 */
public class TestSqlEval extends TestCase {

    String helpProcess(String sql, QueryMetadataInterface metadata, ProcessorDataManager dataMgr) 
        throws Exception {
        
        CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder();
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        
        QueryProcessor.ProcessorFactory factory = new SimpleQueryProcessorFactory(bufferMgr, dataMgr, capFinder, null, metadata);
        
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(factory);
        
        SqlEval sqlEval = new SqlEval(null, cc, null, Collections.EMPTY_MAP);
        Source results =  sqlEval.executeSQL(sql);        
        String result = toXMLString(results);
        sqlEval.close();
        return result;
    }
    
    String toXMLString(Source xmlSrc) throws Exception{
    	StandardXMLTranslator sxt = new StandardXMLTranslator(xmlSrc, null);
    	return sxt.getString();
    }
      
    public void testTableResult() throws Exception { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1"; //$NON-NLS-1$
        
        String expected ="<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$ 
                    "<results>" + //$NON-NLS-1$
                    "<row><e1>a</e1><e2>0</e2><a>false</a><b>2.0</b></row>" + //$NON-NLS-1$
                    "<row><e1>null</e1><e2>1</e2><a>false</a><b>1.0</b></row>" +//$NON-NLS-1$
                    "<row><e1>a</e1><e2>3</e2><a>true</a><b>7.0</b></row>" +//$NON-NLS-1$
                    "<row><e1>c</e1><e2>1</e2><a>true</a><b>null</b></row>" +//$NON-NLS-1$
                    "<row><e1>b</e1><e2>2</e2><a>false</a><b>0.0</b></row>" +//$NON-NLS-1$
                    "<row><e1>a</e1><e2>0</e2><a>false</a><b>2.0</b></row>" +//$NON-NLS-1$
                    "</results>";//$NON-NLS-1$
            
    
        // Construct data manager with data
        FakeDataManager dataMgr = new FakeDataManager();
        FakeDataStore.sampleData1(dataMgr);
        
        String actual = helpProcess(sql, FakeMetadataFactory.example1Cached(), dataMgr);
        assertEquals("Wrong Results", expected, actual); //$NON-NLS-1$
    }
    
    public void testZeroTableResult() throws Exception { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 where e2=4"; //$NON-NLS-1$
        
        String expected ="<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$ 
                    "<results/>"; //$NON-NLS-1$
                
        // Construct data manager with data
        FakeDataManager dataMgr = new FakeDataManager();
        FakeDataStore.sampleData1(dataMgr);
        
        String actual = helpProcess(sql, FakeMetadataFactory.example1Cached(), dataMgr);
        assertEquals("Wrong Results", expected, actual); //$NON-NLS-1$
    }
    
    public void testEntityInResults() throws Exception { 
        // Create query 
        String sql = "SELECT '&', '<'"; //$NON-NLS-1$
        
        String expected ="<?xml version=\"1.0\" encoding=\"UTF-8\"?><results><row><expr>&amp;</expr><expr1>&lt;</expr1></row></results>"; //$NON-NLS-1$
                
        // Construct data manager with data
        FakeDataManager dataMgr = new FakeDataManager();
        FakeDataStore.sampleData1(dataMgr);
        
        String actual = helpProcess(sql, FakeMetadataFactory.example1Cached(), dataMgr);
        assertEquals("Wrong Results", expected, actual); //$NON-NLS-1$
    }
    
    public void testXMLResult() throws Exception {
        String sql = "SELECT * FROM xmltest.doc1"; //$NON-NLS-1$
        
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n" + //$NON-NLS-1$
            "   <Catalog>\n" +  //$NON-NLS-1$
            "      <Items>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"001\">\n" +  //$NON-NLS-1$
            "            <Name>Lamp</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>5</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"002\">\n" +  //$NON-NLS-1$
            "            <Name>Screwdriver</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>100</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"003\">\n" +  //$NON-NLS-1$
            "            <Name>Goat</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>4</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "      </Items>\n" +  //$NON-NLS-1$
            "   </Catalog>\n" +  //$NON-NLS-1$
            "</Catalogs>"; //$NON-NLS-1$
        
        String actual = helpProcess(sql, metadata, dataMgr);
        assertEquals("Wrong Results", expected, actual); //$NON-NLS-1$
    }   
       
}
