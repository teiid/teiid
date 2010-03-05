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

package com.metamatrix.connector.xml.base;

import java.util.List;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.ColumnReference;
import org.teiid.connector.language.Comparison;
import org.teiid.connector.language.Condition;
import org.teiid.connector.language.DerivedColumn;
import org.teiid.connector.language.Expression;
import org.teiid.connector.language.LanguageUtil;
import org.teiid.connector.language.Literal;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.Column;


/**
 *
 */
public class TestOutputXPathDesc extends TestCase {

    
	private static String vdbPath;
    private static final String QUERY = "select OutputColumn from CriteriaDescTable where"
    		+ " OutputColumn in ('foo') order by OutputColumn";
   
    static {
    	vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
    }
    
//  removing hansel while testing clover    
    
/*    
    public static Test suite() {
    	return new CoverageDecorator(OutputXPathDescTest.class, new Class[] {OutputXPathDesc.class});    	
    }
*/    
    
    /**
     * Constructor for OutputXPathDescTest.
     * @param arg0
     */
    public TestOutputXPathDesc(String arg0) {
        super(arg0);
    }

    /*
     * Class under test for void OutputXPathDesc(Element)
     */
    public void testOutputXPathDescElement() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        OutputXPathDesc desc = new OutputXPathDesc(element);
        assertNull(desc.getCurrentValue());
        assertNotNull(desc.getDataType());;
    }
    
    public void testOutputXPathDescParam() throws Exception {
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam in ('foo')";
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        OutputXPathDesc desc = new OutputXPathDesc(element);
        assertNotNull("OutputXPathDesc is null", desc);
    }

    
    public void testOutputXPathDescNoXPath() throws Exception {
        try {
        	String query = "select OutputColumnNoXPath from CriteriaDescTable";
        	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	DerivedColumn symbol = (DerivedColumn) iquery.getDerivedColumns().get(colLocation);
        	Expression expr = symbol.getExpression();
        	assertTrue(expr instanceof ColumnReference);
        	Column element = ((ColumnReference) expr).getMetadataObject(); 
            OutputXPathDesc desc = new OutputXPathDesc(element);
            fail("should not be able to create OuputXPathDesc with no XPath");
        } catch (ConnectorException ce) {
        	return;
        }
    }
    /*
     * Class under test for void OutputXPathDesc(ILiteral)
     */
    public void testOutputXPathDescILiteral() throws Exception { 
    	String strLiteral = "MetaMatrix";
    	String strQuery = "Select Company_id from Company where Company_id = '" + strLiteral + "'";
    	Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
    	Condition crits = query.getWhere();
    	List criteriaList = LanguageUtil.separateCriteriaByAnd(crits);
        Comparison compCriteria = (Comparison) criteriaList.get(0);                  	
        Literal literal = (Literal) compCriteria.getRightExpression();
    	OutputXPathDesc desc = new OutputXPathDesc(literal);
    	assertNotNull(desc);
    	assertEquals(strLiteral, desc.getCurrentValue().toString());
    	assertEquals(strLiteral.getClass(), desc.getDataType());
    }
    
    public void testOutputXPathDescILiteralNullValue() throws Exception { 
    	String strLiteral = "MetaMatrix";
    	String strQuery = "Select Company_id from Company where Company_id = '" + strLiteral + "'";
    	Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
    	Condition crits = query.getWhere();
    	List criteriaList = LanguageUtil.separateCriteriaByAnd(crits);
        Comparison compCriteria = (Comparison) criteriaList.get(0);                  	
        Literal literal = (Literal) compCriteria.getRightExpression();
        literal.setValue(null);
    	OutputXPathDesc desc = new OutputXPathDesc(literal);
    	assertNotNull(desc);
    	assertNull(desc.getCurrentValue());
    	assertEquals(strLiteral.getClass(), desc.getDataType());
    }

    public void testSetAndGetCurrentValue() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        OutputXPathDesc desc = new OutputXPathDesc(element);
        String myVal = "myValue";
        desc.setCurrentValue(myVal);
        assertEquals(myVal, desc.getCurrentValue());
    }

    public void testGetDataType() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        OutputXPathDesc desc = new OutputXPathDesc(element);         
        assertNotNull(desc.getDataType());
        assertEquals(String.class, desc.getDataType());
    }

}
