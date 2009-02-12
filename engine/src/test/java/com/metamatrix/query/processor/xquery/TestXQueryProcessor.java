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

package com.metamatrix.query.processor.xquery;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.FakeDataStore;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.processor.dynamic.SimpleQueryProcessorFactory;
import com.metamatrix.query.processor.xml.TestXMLProcessor;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;

/**
 * Tests processing XQueries
 */
public class TestXQueryProcessor extends TestCase {
	
    public TestXQueryProcessor(String name) {
        super(name);
    }

    private void helpTestProcess(String xquery, String expectedResult, FakeMetadataFacade metadata, FakeDataManager dataMgr) throws Exception{
    	helpProcess(xquery, new List[] {Arrays.asList(expectedResult)}, dataMgr, metadata);
    }
    
	private void helpTest(String sql, List[] expected) throws MetaMatrixComponentException {
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleXQueryTransformations();

        helpProcess(sql, expected, dataManager, metadata);
	}

	private void helpProcess(String sql, List[] expected,
			FakeDataManager dataManager, FakeMetadataFacade metadata)
			throws MetaMatrixComponentException {
		ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new SimpleQueryProcessorFactory(BufferManagerFactory.getStandaloneBufferManager(), dataManager, new DefaultCapabilitiesFinder(), null, metadata));
        TestProcessor.helpProcess(plan, cc, dataManager, expected);
	}

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
        
        helpTestProcess(xquery, expectedDoc, metadata, dataMgr);         
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

        helpTestProcess(xquery, expected, metadata, dataMgr);
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

        helpTestProcess(xquery, expected, metadata, dataMgr);        
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

        helpTestProcess(xquery, expectedDoc, metadata, dataMgr); 
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

        helpTestProcess(xquery, expectedDoc, metadata, dataMgr); 
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

        helpTestProcess(xquery, expectedDoc, metadata, dataMgr);
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

        helpTestProcess(xquery, expectedDoc, metadata, dataMgr);
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

        helpTestProcess(xquery, expectedDoc, metadata, dataMgr);
    }    
    
    /** 
     * defect 12405 - Using <Item name="{$x}" /> instead of
     * <Item name="{$x/text()}" /> in line 4 of xquery seems
     * to cause Exception.
     */
    public void test_defect12405() throws Exception {
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
				        "return  <Item name=\"{$x}\" />\r\n" + //$NON-NLS-1$
				        "}\r\n" + //$NON-NLS-1$
				        "</Items>\r\n"; //$NON-NLS-1$
        
        helpTestProcess(xquery, expectedDoc, metadata, dataMgr);         
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
        
        helpTestProcess(xquery, expectedDoc, metadata, dataMgr);         
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
        
        helpTestProcess(xquery, expected, metadata, dataMgr);         
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
        
        helpTestProcess(xquery, expected, metadata, dataMgr);         
    }     
    
    public void testXQueryView1() throws Exception {
        // Create query 
        String sql = "exec m.xproc1()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><test/>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }
    
    public void testXQueryView2() throws Exception {
        // Create query 
        String sql = "exec m.xproc2()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><test/>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

    public void testXQueryViewWithXMLInput() throws Exception {
        // Create query 
        String sql = "exec m.xproc3('<test/>')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><wrap><test/></wrap>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

    public void testXQueryViewWithScalarInputs() throws Exception {
        // Create query 
        String sql = "exec m.xproc4('abc', 10)"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><wrap><a>abc</a><b>10</b></wrap>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

    public void testXQueryViewWithScalarInputCallingDocWithBuiltSQL() throws Exception {
        // Create query - this query calls the specified proc and wraps the data 
        String sql = "exec m.xproc5('m.xproc1')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><wrap><test/></wrap>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

    public void testXQueryViewCallingMultipleDocs() throws Exception {
        // Create query - this query calls the specified proc and wraps the data 
        String sql = "exec m.combinetags('xyz', 't1', 'a', 't2', 'b')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><xyz><t1>a</t1><t2>b</t2></xyz>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

    public void testXQueryViewWithXMLInAndOut() throws Exception {
        // Create query - this query calls the specified proc and wraps the data 
        String sql = "exec m.svc8('<in><tag>mytag</tag><value>myvalue</value></in>')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><mytag>myvalue</mytag>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

    public void testNestedXQueryViews() throws Exception {
        // Create query - this query calls the specified proc and wraps the data 
        String sql = "exec m.svc9('<in><tag>animal</tag><values><value>zebra</value><value>newt</value><value>lemur</value></values></in>')"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><results><animal>zebra</animal><animal>newt</animal><animal>lemur</animal></results>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

    public void testXQueryWithFunctionDefinition() throws Exception {
        // Create query - this query calls the specified proc and wraps the data 
        String sql = "declare namespace mm = \"http://www.metamatrix.com\";\n" + //$NON-NLS-1$
                     "declare function mm:square($x as xs:integer) as xs:integer {\n" + //$NON-NLS-1$
                     "  $x * $x\n" + //$NON-NLS-1$
                     "};\n" + //$NON-NLS-1$
                     "<out>{ mm:square(5) }</out>"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><out>25</out>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }
    
    public void testXQueryView10() throws Exception {
        // Create query 
        String sql = "exec m.xproc10()"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><test/>"}), //$NON-NLS-1$
        };    
    
        helpTest(sql, expected);
    }

}
