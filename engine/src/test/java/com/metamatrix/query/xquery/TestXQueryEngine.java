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

package com.metamatrix.query.xquery;

import java.io.File;
import java.net.URI;
import java.sql.SQLXML;
import java.util.HashMap;

import junit.framework.TestCase;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.xquery.saxon.SaxonXQueryExpression;

/**
 * Test XQueryEngine and XQueryExpression implementations
 */
public class TestXQueryEngine extends TestCase {

    /**
     * constructor
     */
    public TestXQueryEngine() {
        super();
    }

    /**
     * @param name
     */
    public TestXQueryEngine(String name) {
        super(name);
    }

// =========================================================================
// HELPERS
// =========================================================================
    private void helpTestEngine(String xQuery, SQLXML expected, XQuerySQLEvaluator sqlEval) throws Exception{
        XQueryExpression expr = new SaxonXQueryExpression();
        helpTestEngine(expr, xQuery, expected, sqlEval);
    }
    
    private void helpTestEngine(XQueryExpression expr, String xQuery, SQLXML expected, XQuerySQLEvaluator sqlEval) throws Exception{
        expr.compileXQuery(xQuery);        
        SQLXML actualResults = expr.evaluateXQuery(sqlEval);
        assertEquals(expected.getString(), actualResults.getString());        
    }

    private void helpTestEngineFails(String xQuery, Class expectedFailure, XQuerySQLEvaluator sqlEval) throws Exception{
        XQueryExpression expr = new SaxonXQueryExpression();
        expr.compileXQuery(xQuery);
        
        try {
            expr.evaluateXQuery(sqlEval);
            fail("expected failure");  //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            assertEquals(expectedFailure, e.getClass());
        }
    }

    
// =========================================================================
// TESTS
// =========================================================================

    /** simple test */
    public void test1() throws Exception {
        
        // Construct a JDOM tree, like we use in the server
        Element element = new Element("test");//$NON-NLS-1$
        Element element2 = new Element("name");//$NON-NLS-1$
        element2.addContent("Tim");//$NON-NLS-1$
        element.addContent(element2);
        element2 = new Element("name");//$NON-NLS-1$
        element2.addContent("Tom");//$NON-NLS-1$
        element.addContent(element2);
        element2 = new Element("name");//$NON-NLS-1$
        element2.addContent("Bill");//$NON-NLS-1$
        element.addContent(element2);
        Document doc = new Document(element);
        XMLOutputter xmlOutputter = new XMLOutputter();

        String xquery = "<friends>\n" +  //$NON-NLS-1$
                        "{\n" +  //$NON-NLS-1$
//                        "let $y := doc(\"goo\")//names\n" +  //$NON-NLS-1$
                        "for $x in doc(\"foo\")//name\n" +  //$NON-NLS-1$
                        "return  <friend>{$x/text()}</friend>\n" +  //$NON-NLS-1$
                        "}\n" +  //$NON-NLS-1$
                        "</friends>"; //$NON-NLS-1$   
                        
        String output = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                        "<friends>" +  //$NON-NLS-1$
                        "<friend>Tim</friend>" +   //$NON-NLS-1$
                        "<friend>Tom</friend>" +  //$NON-NLS-1$
                        "<friend>Bill</friend>" +  //$NON-NLS-1$
                        "</friends>"; //$NON-NLS-1$                        
        
        helpTestEngine(xquery, new SQLXMLImpl(output), new HardcodedSqlEval(xmlOutputter.outputString(doc)));
    }

    /** defect 12387 */
    public void testDefect12387() throws Exception {
        
        // Construct a JDOM tree, like we use in the server
        Element element = new Element("test");//$NON-NLS-1$
        Element element2 = new Element("name");//$NON-NLS-1$
        element2.addContent("Tim");//$NON-NLS-1$
        element.addContent(element2);
        element2 = new Element("name");//$NON-NLS-1$
        element2.addContent("Tom");//$NON-NLS-1$
        element.addContent(element2);
        element2 = new Element("name");//$NON-NLS-1$
        element2.addContent("Bill");//$NON-NLS-1$
        element.addContent(element2);
        Document doc = new Document(element);
        XMLOutputter xmlOutputter = new XMLOutputter();
        
        String xquery = "<friends>\n" +  //$NON-NLS-1$
                        "{\n" +  //$NON-NLS-1$
                        "for $x in doc(foo)//name\n" +  //$NON-NLS-1$
                        "return  <friend>{$x/text()}</friend>\n" +  //$NON-NLS-1$
                        "}\n" +  //$NON-NLS-1$
                        "</friends>"; //$NON-NLS-1$   
        
        helpTestEngineFails(xquery, MetaMatrixProcessingException.class, new HardcodedSqlEval(xmlOutputter.outputString(doc)));
    }
    
    public void testDocArgReadingFileURI() throws Exception {
        File f = UnitTestUtil.getTestDataFile("testExample.xml"); //$NON-NLS-1$
        URI uri = f.toURI();
        String xquery = "doc(\"" + uri.toString() + "\")"; //$NON-NLS-1$ //$NON-NLS-2$
        
        FileUtil util = new FileUtil(f.getCanonicalPath());
        String expectedStr = util.read().replaceAll("[ \t\r\n]", ""); //$NON-NLS-1$ //$NON-NLS-2$
        
        XQueryExpression expr = new SaxonXQueryExpression();
        expr.compileXQuery(xquery);
        SQLXML actualResults = expr.evaluateXQuery(new HardcodedSqlEval(null));
        
        assertEquals(expectedStr, actualResults.getString().replaceAll("[ \t\r\n]", "")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInputParams() throws Exception {
        String sql = "SELECT * FROM xmltest.doc1"; //$NON-NLS-1$
        String xquery = "declare variable $itemid as xs:string external;" + //$NON-NLS-1$
                        "<set>\n" +  //$NON-NLS-1$
                        "{\n" +  //$NON-NLS-1$                        
                        "for $x in doc(\""+sql+"\")/Catalogs/Catalog/Items\n" +  //$NON-NLS-1$ //$NON-NLS-2$
                        "return  <Name>{$x/Item[@ItemID=$itemid]/Name/text()}</Name>\n" +  //$NON-NLS-1$
                        "}\n" +  //$NON-NLS-1$
                        "</set>"; //$NON-NLS-1$   
 
      String inputdoc = 
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
      
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                        "<set>" +  //$NON-NLS-1$
                        "<Name>Lamp</Name>" +   //$NON-NLS-1$
                        "</set>"; //$NON-NLS-1$                        
        XQueryExpression expr = new SaxonXQueryExpression();
        HashMap params= new HashMap();
        params.put("ItemID", new Constant("001")); //$NON-NLS-1$ //$NON-NLS-2$
        expr.setParameters(params);
        helpTestEngine(expr, xquery, new SQLXMLImpl(expected), new HardcodedSqlEval(inputdoc));        
    }     

}
