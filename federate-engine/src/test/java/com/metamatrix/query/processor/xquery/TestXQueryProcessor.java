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

package com.metamatrix.query.processor.xquery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.FakeDataStore;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.dynamic.SqlEval;
import com.metamatrix.query.processor.xml.TestXMLProcessor;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;

/**
 * Tests processing XQueries
 */
public class TestXQueryProcessor extends TestCase {
	private static final boolean DEBUG = false;
    public TestXQueryProcessor(String name) {
        super(name);
    }

// =========================================================================
// HELPERS
// =========================================================================

    /**
     * help test processing XQuery
     * @param xquery XQuery to process
     * @param expectedResult expected result String
     * @param expectedXMLPlans expected number of child XML plans to be planned for input XML docs 
     * @param metadata QueryMetadataInterface
     * @param dataMgr FakeDataManager
     * @throws Exception
     */
    private static void helpTestProcess(String xquery, String expectedResult, int expectedXMLPlans, FakeMetadataFacade metadata, FakeDataManager dataMgr) throws Exception{

//    System.out.println(xquery);
//    System.out.println(expectedResult);
        helpTestProcess(xquery, expectedResult, expectedXMLPlans, metadata, dataMgr, true, MetaMatrixComponentException.class, null);
    }

    private static void helpTestProcess(String xquery, String expectedResult, int expectedXMLPlans, FakeMetadataFacade metadata, FakeDataManager dataMgr, final boolean shouldSucceed, Class expectedException, final String shouldFailMsg) throws Exception{
        Command command = helpGetCommand(xquery, metadata);
       
        if (shouldSucceed){
            
            AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
            XQueryPlan plan = (XQueryPlan)QueryOptimizer.optimizePlan(command, metadata, null, new DefaultCapabilitiesFinder(), analysisRecord, null);
            //System.out.println("Plan w/ criteria\n" + plan);        
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());
            }           
            
            //assertTrue(plan.getXmlPlans().size() == expectedXMLPlans);
            
            BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
            List schema = plan.getOutputElements();
            ArrayList typeNames = new ArrayList();
            for(Iterator s = schema.iterator(); s.hasNext();) {
                SingleElementSymbol es = (SingleElementSymbol)s.next();            
                typeNames.add(DataTypeManager.getDataTypeName(es.getType()));
            }
            String[] types = (String[])typeNames.toArray(new String[typeNames.size()]);              
            TupleSourceID tsID = bufferMgr.createTupleSource(plan.getOutputElements(), types, null, TupleSourceType.FINAL);
            CommandContext context = new CommandContext("pID", null, tsID, 10, null, null, null, null);                                            //$NON-NLS-1$
            QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataMgr);
    
            while(true) {
                try {
                    processor.process();
                    break;
                } catch(BlockedException e) {
                    // retry
                }
            }
        
    
            int count = bufferMgr.getFinalRowCount(tsID);
            assertEquals("Incorrect number of records: ", 1, count); //$NON-NLS-1$
            
            TupleSource ts = bufferMgr.getTupleSource(tsID);
            List row = ts.nextTuple();
            assertEquals("Incorrect number of columns: ", 1, row.size()); //$NON-NLS-1$
            
            XMLType id =  (XMLType)row.get(0); 
            String actualDoc = id.getString(); 

            bufferMgr.removeTupleSource(tsID);

//           System.out.println("expectedDoc = \n" + expectedDoc);
//           System.out.println("actualDoc = \n" + actualDoc);
            
//           for (int i=0,j=0; i<expectedDoc.length(); i++, j++) {
//               if (expectedDoc.charAt(i) == actualDoc.charAt(j)) {
//               } else {
//                   System.out.println(" i = " + i + " expected = " + expectedDoc.charAt(i) + " actual = " + actualDoc.charAt(i));
//               }
//           }
            assertEquals("XML doc mismatch: ", expectedResult, actualDoc); //$NON-NLS-1$
        } else {
            
            Exception expected = null;
            try{
                AnalysisRecord analysisRecord = new AnalysisRecord(false, false, DEBUG);
                XQueryPlan plan = (XQueryPlan)QueryOptimizer.optimizePlan(command, metadata, null, new DefaultCapabilitiesFinder(), analysisRecord, null);
//System.out.println("Plan w/ criteria\n" + plan);        
                if(DEBUG) {
                    System.out.println(analysisRecord.getDebugLog());
                }           
        
                BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
                TupleSourceID tsID = bufferMgr.createTupleSource(plan.getOutputElements(), null, null, TupleSourceType.FINAL);
                CommandContext context = new CommandContext("pID", null, tsID, 10, null, null, null, null);                                                                 //$NON-NLS-1$
                QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataMgr);
                processor.process();
            } catch (Exception e){
              
                if (expectedException.isInstance(e)){
                    expected = e;
                   
                }
            }
            
            assertNotNull(shouldFailMsg, expected);
        }
    }

    private static Command helpGetCommand(String sql, FakeMetadataFacade metadata) { 
        Command command = null;
        
        // parse
        try { 
            QueryParser parser = new QueryParser();
            command = parser.parseCommand(sql);
        } catch(Throwable e) { 
            e.printStackTrace();
            fail("Exception during parsing (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        }    
        
        // resolve
        try { 
            QueryResolver.resolveCommand(command, metadata);
        } catch(Throwable e) { 
            e.printStackTrace();
            fail("Exception during resolution (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        } 

        // rewrite
        try { 
            command = QueryRewriter.rewrite(command, null, metadata, null);
        } catch(Throwable e) { 
            e.printStackTrace();
            fail("Exception during rewriting (" + e.getClass().getName() + "): " + e.getMessage());     //$NON-NLS-1$ //$NON-NLS-2$
        } 
        
        return command;
    }

    private CommandContext createCommandContext() {
        Properties props = new Properties();
        CommandContext context = new CommandContext("0", "test", null, 5, "user", null, null, "myvdb", "1", props, false, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        context.setProcessorBatchSize(2000);
        context.setConnectorBatchSize(2000);
        return context;
    } 
    
    SqlEval helpGetSQLEval(QueryMetadataInterface metadata, ProcessorDataManager dataMgr) 
        throws Exception {    
        CommandContext context = createCommandContext();       
        CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder();
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();        
        return new SqlEval(metadata, context, capFinder, null, bufferMgr, dataMgr);
    }    
// =========================================================================
// TESTS
// =========================================================================

    /** simple test */
    public void testSingleInputDoc() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);
         
//        String expectedDoc = 
//            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
//            "<Root>\r\n" + //$NON-NLS-1$
//            "    <ItemName>Lamp</ItemName>\r\n" +  //$NON-NLS-1$
//            "    <ItemName>Screwdriver</ItemName>\r\n" +  //$NON-NLS-1$
//            "    <ItemName>Goat</ItemName>\r\n" +  //$NON-NLS-1$
//            "</Root>\r\n\r\n"; //$NON-NLS-1$

        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item>Lamp</Item>" + //$NON-NLS-1$
                   "<Item>Screwdriver</Item>" + //$NON-NLS-1$
                   "<Item>Goat</Item>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$

//        helpTestProcess("SELECT * FROM xmltest.doc9893", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$

        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$
        
        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr);         
    }

    public void testInputDocProducingMultipleResults() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
         
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
            "<Items>" + //$NON-NLS-1$
               "<Item>Lamp</Item>" + //$NON-NLS-1$
            "</Items>"; //$NON-NLS-1$
        
        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc11\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/Name/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        helpTestProcess(xquery, expected, -1, metadata, dataMgr, true, MetaMatrixComponentException.class, "");         //$NON-NLS-1$
    }

    public void testInputDocProducingZeroResults() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Items/>"; //$NON-NLS-1$         
        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc11 where Item.Name='Plutonium'\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        helpTestProcess(xquery, expected, -1, metadata, dataMgr, true, MetaMatrixComponentException.class, "");         //$NON-NLS-1$
    }

    public void testInputDocFullQuery() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);

//      String expectedDoc = 
//          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
//          "<Root>\r\n" + //$NON-NLS-1$
//          "    <ItemName>Lamp</ItemName>\r\n" +  //$NON-NLS-1$
//          "    <ItemName>Screwdriver</ItemName>\r\n" +  //$NON-NLS-1$
//          "    <ItemName>Goat</ItemName>\r\n" +  //$NON-NLS-1$
//          "</Root>\r\n\r\n"; //$NON-NLS-1$

         
//        String expectedDoc3 = 
//            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
//            "<Item ItemID=\"003\">\r\n" +  //$NON-NLS-1$
//            "    <Name>Goat</Name>\r\n" +  //$NON-NLS-1$
//            "    <Quantity>4</Quantity>\r\n" +  //$NON-NLS-1$
//            "    <Suppliers>\r\n" + //$NON-NLS-1$
//            "        <Supplier SupplierID=\"56\">\r\n" + //$NON-NLS-1$
//            "            <Name>Microsoft</Name>\r\n" + //$NON-NLS-1$
//            "            <Zip>66666</Zip>\r\n" + //$NON-NLS-1$
//            "        </Supplier>\r\n" + //$NON-NLS-1$
//            "    </Suppliers>\r\n" + //$NON-NLS-1$
//            "</Item>\r\n\r\n"; //$NON-NLS-1$
         
        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item>Goat</Item>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$
         
        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc11 where Item.Name='Goat'\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/Name/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr); 
    }

    public void testInputDocFullQuery2() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);

        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item>Goat</Item>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$
         
        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc11 where Item.@ItemID='003'\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/Name/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr); 
    }

    public void testTwoInputDocs() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);

        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item>Lamp</Item>" + //$NON-NLS-1$
                   "<Item>Screwdriver</Item>" + //$NON-NLS-1$
                   "<Item>Goat</Item>" + //$NON-NLS-1$
                   "<Item2>Lamp</Item2>" + //$NON-NLS-1$
                   "<Item2>Screwdriver</Item2>" + //$NON-NLS-1$
                   "<Item2>Goat</Item2>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$

        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc1Unformatted\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Item2>{$x/Name/text()}</Item2>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        int expectedXMLPlans = 2;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr);
    }

    public void testSameInputDocTwice() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);

        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item>Lamp</Item>" + //$NON-NLS-1$
                   "<Item>Screwdriver</Item>" + //$NON-NLS-1$
                   "<Item>Goat</Item>" + //$NON-NLS-1$
                   "<Quantity>5</Quantity>" + //$NON-NLS-1$
                   "<Quantity>100</Quantity>" + //$NON-NLS-1$
                   "<Quantity>4</Quantity>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$

        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc1Unformatted\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/Name/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $y in doc(\"select * from xmltest.doc1Unformatted\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Quantity>{$y/Quantity/text()}</Quantity>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr);
    }
    
    public void testSameInputDocTwice2() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);

        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item>Lamp</Item>" + //$NON-NLS-1$
                   "<Item>Screwdriver</Item>" + //$NON-NLS-1$
                   "<Item>Goat</Item>" + //$NON-NLS-1$
                   "<Quantity>5</Quantity>" + //$NON-NLS-1$
                   "<Quantity>100</Quantity>" + //$NON-NLS-1$
                   "<Quantity>4</Quantity>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$

        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc1Unformatted\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/Name/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $y in doc(\"select * from XMLTEST.DOC1UNFORMATTED\")//Item\r\n" + //$NON-NLS-1$
                        "return  <Quantity>{$y/Quantity/text()}</Quantity>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr);
    }    
    
    /** 
     * defect 12405 - Using <Item name="{$x}" /> instead of
     * <Item name="{$x/text()}" /> in line 4 of xquery seems
     * to cause Exception.
     */
    public void DEFER_test_defect12405() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);
         
//      input doc:
//            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
//            "<Root>\r\n" + //$NON-NLS-1$
//            "    <ItemName>Lamp</ItemName>\r\n" +  //$NON-NLS-1$
//            "    <ItemName>Screwdriver</ItemName>\r\n" +  //$NON-NLS-1$
//            "    <ItemName>Goat</ItemName>\r\n" +  //$NON-NLS-1$
//            "</Root>\r\n\r\n"; //$NON-NLS-1$

        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item name=\"Lamp\"/>" + //$NON-NLS-1$
                   "<Item name=\"Screwdriver\"/>" + //$NON-NLS-1$
                   "<Item name=\"Goat\"/>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$

        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item name=\"{$x}\" />\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$
        
        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr);         
    }    

    /** defect 12405 */
    public void test_defect12405_2() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);
         
//      input doc:
//            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
//            "<Root>\r\n" + //$NON-NLS-1$
//            "    <ItemName>Lamp</ItemName>\r\n" +  //$NON-NLS-1$
//            "    <ItemName>Screwdriver</ItemName>\r\n" +  //$NON-NLS-1$
//            "    <ItemName>Goat</ItemName>\r\n" +  //$NON-NLS-1$
//            "</Root>\r\n\r\n"; //$NON-NLS-1$

        String expectedDoc = 
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<Items>" + //$NON-NLS-1$
                   "<Item name=\"Lamp\"/>" + //$NON-NLS-1$
                   "<Item name=\"Screwdriver\"/>" + //$NON-NLS-1$
                   "<Item name=\"Goat\"/>" + //$NON-NLS-1$
                "</Items>"; //$NON-NLS-1$

        String xquery = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"select * from xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item name=\"{$x/text()}\" />\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$
        
        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expectedDoc, expectedXMLPlans, metadata, dataMgr);         
    }  

    public void testExecutingSQLQuery() throws Exception {
        String sql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1"; //$NON-NLS-1$
        
        String xquery = "<set>\n" +  //$NON-NLS-1$
                        "{\n" +  //$NON-NLS-1$
                        "for $x in doc(\""+sql+"\")/results/row/b\n" +  //$NON-NLS-1$ //$NON-NLS-2$
                        "return  <b>{$x/text()}</b>\n" +  //$NON-NLS-1$
                        "}\n" +  //$NON-NLS-1$
                        "</set>"; //$NON-NLS-1$   
                        
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                        "<set>" +  //$NON-NLS-1$
                        "<b>2.0</b>" +   //$NON-NLS-1$
                        "<b>1.0</b>" +  //$NON-NLS-1$
                        "<b>7.0</b>" +  //$NON-NLS-1$
                        "<b>null</b>" +  //$NON-NLS-1$
                        "<b>0.0</b>" +  //$NON-NLS-1$
                        "<b>2.0</b>" +  //$NON-NLS-1$                        
                        "</set>"; //$NON-NLS-1$                        
        
        FakeDataManager dataMgr = new FakeDataManager();
        FakeDataStore.sampleData1(dataMgr);
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        
        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expected, expectedXMLPlans, metadata, dataMgr);         
    }     

    public void testExecutingXMLQuery() throws Exception {
        String sql = "SELECT * FROM xmltest.doc1"; //$NON-NLS-1$

//        Input Doc        
//        String expected = 
//            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +  //$NON-NLS-1$
//            "<Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n" + //$NON-NLS-1$
//            "   <Catalog>\n" +  //$NON-NLS-1$
//            "      <Items>\n" +  //$NON-NLS-1$
//            "         <Item ItemID=\"001\">\n" +  //$NON-NLS-1$
//            "            <Name>Lamp</Name>\n" +  //$NON-NLS-1$
//            "            <Quantity>5</Quantity>\n" +  //$NON-NLS-1$
//            "         </Item>\n" +  //$NON-NLS-1$
//            "         <Item ItemID=\"002\">\n" +  //$NON-NLS-1$
//            "            <Name>Screwdriver</Name>\n" +  //$NON-NLS-1$
//            "            <Quantity>100</Quantity>\n" +  //$NON-NLS-1$
//            "         </Item>\n" +  //$NON-NLS-1$
//            "         <Item ItemID=\"003\">\n" +  //$NON-NLS-1$
//            "            <Name>Goat</Name>\n" +  //$NON-NLS-1$
//            "            <Quantity>4</Quantity>\n" +  //$NON-NLS-1$
//            "         </Item>\n" +  //$NON-NLS-1$
//            "      </Items>\n" +  //$NON-NLS-1$
//            "   </Catalog>\n" +  //$NON-NLS-1$
//            "</Catalogs>"; //$NON-NLS-1$
        
        String xquery = "<set>\n" +  //$NON-NLS-1$
                        "{\n" +  //$NON-NLS-1$
                        "for $x in doc(\""+sql+"\")/Catalogs/Catalog/Items/Item\n" +  //$NON-NLS-1$ //$NON-NLS-2$
                        "return  <Name>{$x/Name/text()}</Name>\n" +  //$NON-NLS-1$
                        "}\n" +  //$NON-NLS-1$
                        "</set>"; //$NON-NLS-1$   
                        
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                        "<set>" +  //$NON-NLS-1$
                        "<Name>Lamp</Name>" +   //$NON-NLS-1$
                        "<Name>Screwdriver</Name>" +  //$NON-NLS-1$
                        "<Name>Goat</Name>" +  //$NON-NLS-1$
                        "</set>"; //$NON-NLS-1$                        
        
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManager(metadata);
        
        int expectedXMLPlans = 1;
        helpTestProcess(xquery, expected, expectedXMLPlans, metadata, dataMgr);         
    }     
}
