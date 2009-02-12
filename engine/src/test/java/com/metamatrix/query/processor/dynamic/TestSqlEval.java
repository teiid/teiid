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

import java.io.BufferedReader;

import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;

import junit.framework.TestCase;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.FakeDataStore;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.processor.xml.TestXMLProcessor;
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
        
        SqlEval sqlEval = new SqlEval(bufferMgr, cc, null);
        Source results =  sqlEval.executeSQL(sql);        
        String result = toXMLString(results);
        sqlEval.close();
        return result;
    }
    
    String toXMLString(Source xmlSrc) throws Exception{
        if (xmlSrc instanceof StreamSource) {
            StreamSource input = (StreamSource)xmlSrc;
            BufferedReader reader = new BufferedReader(input.getReader());
            StringBuffer sb = new StringBuffer();
            
            int line = reader.read();
            while(line != -1) {
                sb.append((char)line);
                line = reader.read();
            }
            reader.close();
            return sb.toString();            
        }
        else if (xmlSrc instanceof SAXSource) {
            SAXSource input = (SAXSource)xmlSrc;
            final StringBuffer sb = new StringBuffer();
            ContentHandler handler = new DefaultHandler() {

                public void characters(char[] ch,int start,int length) throws SAXException {
                    sb.append(ch, start, length);
                }
                public void endDocument() throws SAXException {
                    super.endDocument();
                }
                public void endElement(String uri,String localName,String qName) throws SAXException {
                    sb.append("</").append(qName).append(">"); //$NON-NLS-1$ //$NON-NLS-2$
                }
                public void error(SAXParseException e) throws SAXException {
                    super.error(e);
                }
                public void startDocument() throws SAXException {
                    super.startDocument();
                }
                public void startElement(String uri,String localName,String qName, Attributes attributes) throws SAXException {
                    sb.append("<").append(qName).append(">"); //$NON-NLS-1$ //$NON-NLS-2$
                }                
            };
            input.getXMLReader().setContentHandler(handler);
            input.getXMLReader().parse(input.getInputSource());
            return sb.toString();
        }
        return ""; //$NON-NLS-1$
    }
      
    public void testTableResult() throws Exception { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1"; //$NON-NLS-1$
        
        String expected ="" + //$NON-NLS-1$ 
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
        
        String expected ="" + //$NON-NLS-1$ 
                    "<results>" + //$NON-NLS-1$
                    "</results>";//$NON-NLS-1$
                
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
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +  //$NON-NLS-1$
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
