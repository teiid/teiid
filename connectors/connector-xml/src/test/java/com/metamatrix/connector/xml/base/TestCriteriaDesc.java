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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IBaseInCriteria;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.LanguageUtil;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/**
 *
 */
public class TestCriteriaDesc extends TestCase {
    
    private static String vdbPath;
    private static final String QUERY = "select RequiredDefaultedParam from CriteriaDescTable " 
    		+ "where RequiredDefaultedParam in ('foo') order by RequiredDefaultedParam";
    private static final String VALUE = "value1";
   
    
    
    static {
    	vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
    }
    
    public TestCriteriaDesc() {
    	super();
    	System.setProperty("metamatrix.config.none", "true");
    }
    
    /**
     * Constructor for CriteriaDescTest.
     * @param arg0
     */
    public TestCriteriaDesc(String arg0) {
        super(arg0);
        System.setProperty("metamatrix.config.none", "true");
    }

    public void testGetCriteriaDescForColumn() throws Exception {  
    	//case 1: values provided
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam in ('foo')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }

    public void testGetCriteriaDescForColumnDefaultedValue() throws Exception {
    	//case 2: param, required, defaulted
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnNoCriteria() throws Exception {
    	//case 3: param, not required, not defaulted, not allowed empty
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNull("CriteriaDesc is not null", desc);
    }
    
    public void testGetCriteriaDescForColumnAllowEmpty() throws Exception {
    	//case 4: param, not required, not defaulted, allowed empty
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select OptionalAllowedEmptyParam from CriteriaDescTable";
    	final int colLocation = 0;
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnError() {
    	//case 5: param, required, not defaulted
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredUndefaultedParam from CriteriaDescTable";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	Element elem = ((IElement) expr).getMetadataObject();
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("exception not thrown");
        } catch (ConnectorException ce) {
        }   	
    }
    
    public void testGetCriteriaDescForColumnNotParam() throws Exception {
    	//case 6: not a param
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select OutputColumn from CriteriaDescTable";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNull("CriteriaDesc is not null", desc);
    }
    
    public void testGetCriteriaDescForColumnCompare() throws Exception {  
    	//case 7: compare criteria
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam = 'foo'";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    
    public void testGetCriteriaDescForColumnMultiElement() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select MultiElementParam from CriteriaDescTable where MultiElementParam in ('foo','bar')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
    	String multiplicityStr = elem.getProperties().getProperty(
				CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnDelimited() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo','bar')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
    	String multiplicityStr = elem.getProperties().getProperty(
    			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnLikeSearchable() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select LikeSearchableParam from CriteriaDescTable where LikeSearchableParam in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	Element elem = ((IElement) expr).getMetadataObject();
        	String multiplicityStr = elem.getProperties().getProperty(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("should not be able to handle default value");
        } catch (ConnectorException ce) {
        }        
    }
    
    public void testGetCriteriaDescForColumnUnlikeSearchable() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select UnlikeSearchableParam from CriteriaDescTable where UnlikeSearchableParam in ('foo')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
    	String multiplicityStr = elem.getProperties().getProperty(
    			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
    }
    
    public void testGetCriteriaDescForColumnUnsearchable() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select UnsearchableParam from CriteriaDescTable where UnsearchableParam in ('foo','bar')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	Element elem = ((IElement) expr).getMetadataObject();
        	String multiplicityStr = elem.getProperties().getProperty(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
            CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("should not be able to handle default value");
        } catch (ConnectorException ce) {
        }        
    }
    
    public void testGetCriteriaDescForColumnLike() throws Exception {  
    	//case 1: values provided
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam like 'foo'";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }

    public void testGetCriteriaDescForColumnNotEquals() throws Exception {  
    	//case 1: values provided
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam != 'foo'";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where concat(RequiredDefaultedParam, 'bar') in('foo')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnNameMatchFailure() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where AttributeParam in('foo')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    
    public void testGetCriteriaDescForColumnLeftLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable " 
    			+ "where concat('bar', 'foo') = concat('bar', RequiredDefaultedParam)";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnTwoElements() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where OutputColumn = RequiredDefaultedParam";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnLeftElementEqualsLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where AttributeParam = 'foo'";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetCriteriaDescForColumnLeftElementEqualsNonLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where" 
    			+ " RequiredDefaultedParam = concat('foo', OutputColumn)";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc);
    }
    
    public void testGetInputXPathNoXpath() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select BadNoInputXpath from CriteriaDescTable where BadNoInputXpath in ('foo')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals(desc.getColumnName(), desc.getInputXpath());
    }
    
    
    public void testGetInputXPathEmptyXpath() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select BadEmptyInputXPath from CriteriaDescTable where BadEmptyInputXPath in ('foo')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals(desc.getColumnName(), desc.getInputXpath());
    }  
    
    public void testGetDataAttributeNameEmptyName() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath);
    	String query = "select BadNoDataAttributeName from CriteriaDescTable where BadNoDataAttributeName in ('foo')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals("", desc.getDataAttributeName());
    } 
    
    
    

    
    public void testGetInputXpath() {
        try {
        	String query = "select RequiredDefaultedParam from CriteriaDescTable";
        	String inputXPath = "/req/default/value";
            CriteriaDesc desc = createCriteriaDesc(query);
            assertNotNull("CriteriaDesc is null", desc.getInputXpath());
            assertEquals(inputXPath, desc.getInputXpath());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testIsUnlimited() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            assertFalse("Criteria is flagged as unlimited", desc.isUnlimited());            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }                
    }

    public void testIsAutoIncrement() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            assertFalse("criterion is flagged as autoIncrement", desc.isAutoIncrement());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testIsParentAttribute() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            //before its inited
            assertFalse("criterion is flagged as an attribute", desc.isParentAttribute());
            //and after for code coverage
            assertFalse("criterion is flagged as an attribute", desc.isParentAttribute());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testIsEnumeratedAttribute() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            assertFalse("criterion is an enumerated attribute", desc.isEnumeratedAttribute());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testAllowEmptyValueFalse() {
        try {
        	String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable";
            CriteriaDesc desc = createCriteriaDesc(query);
            assertFalse("criterion should not allow for empty values", desc.allowEmptyValue());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
    
    public void testAllowEmptyValueTrue() {
        try {
        	String query = "select OptionalAllowedEmptyParam from CriteriaDescTable";
            CriteriaDesc desc = createCriteriaDesc(query);
            //before init
            assertTrue("criterion should allow for empty values", desc.allowEmptyValue());
            //and after for code coverage
            assertTrue("criterion should allow for empty values", desc.allowEmptyValue());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testIsDataInAttributeFalse() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            assertFalse("criterion is flagged as data in attribute", desc.isDataInAttribute());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
    
    public void testIsDataInAttributeTrue() {
        try {
        	String query = "select AttributeParam from CriteriaDescTable where AttributeParam in ('foo')";
            CriteriaDesc desc = createCriteriaDesc(query);
            assertTrue("criterion is not flagged as data in attribute", desc.isDataInAttribute());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testGetDataAttributeName() {
        try {
        	String query = "select AttributeColumn from TestTable where AttributeColumn in ('foo')";
            Element elem = getElement(query);
            String attributeName = "myAttribute";
            ArrayList list = new ArrayList();
            list.add(VALUE);
            CriteriaDesc desc = new CriteriaDesc(elem, list);
            assertNotNull("CriteriaDesc is null", desc.getDataAttributeName());
            assertTrue("column name mismatch - expected " + attributeName 
            		 + " returned " + desc.getDataAttributeName(), 
					desc.getDataAttributeName().equals(attributeName));            
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testGetValues() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            ArrayList values = desc.getValues();
            assertNotNull("Values list is null", values);
            assertEquals(values.get(0), VALUE);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testGetNumberOfValues() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            assertEquals(1, desc.getNumberOfValues());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testGetCurrentIndexValue() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            assertEquals(VALUE, desc.getCurrentIndexValue());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }
    
    public void testGetCurrentIndexValueEnumerated() throws Exception {
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo', 'bar')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals("There should be two values" , 2, desc.getNumberOfValues());
        assertEquals("foo", desc.getCurrentIndexValue());
        desc.incrementIndex();
        assertEquals("bar", desc.getCurrentIndexValue());
    }
    
    public void testIncrementIndexEnumerated() throws Exception {
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo', 'bar')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
    	Element elem = ((IElement) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertTrue("We should be able to increment this CriteriaDesc", desc.incrementIndex());
    }
    
    public void testGetCurrentIndexValueNoValue() throws Exception {
    	final String query = "select OptionalAllowedEmptyParam from CriteriaDescTable";
        Element elem = getElement(query);
        ArrayList list = new ArrayList();
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        assertEquals("", desc.getCurrentIndexValue());
    }

    public void testGetCurrentIndexValueNoValueNotEmpty() throws Exception {
    	final String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable";
        Element elem = getElement(query);
        ArrayList list = new ArrayList();
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        assertNull(desc.getCurrentIndexValue());
    }
    
    public void testIncrementIndex() throws Exception {
        final String value2 = "value2";
        String query = "select MultiCol from MultiTable where MultiCol in ('" + VALUE + "', '" + value2 + "')";
        Element elem = getElement(query);
        ArrayList list = new ArrayList();
        list.add(VALUE);
        list.add(value2);           
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        assertEquals(VALUE, desc.getCurrentIndexValue());
        assertTrue("index increment failed", desc.incrementIndex());
        assertEquals(value2, desc.getCurrentIndexValue());
        assertFalse("index went beyond number of values", desc.incrementIndex());
    }

    public void testResetIndex() throws Exception {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        desc.resetIndex();
    }

    public void testNameMatch() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        ICriteria crit = query.getWhere();
        List criteriaList = LanguageUtil.separateCriteriaByAnd(crit);        
        Iterator criteriaIter = criteriaList.iterator();
        IExpression expr = null;
        while (criteriaIter.hasNext()) {
            ICriteria criteriaSeg = (ICriteria) criteriaIter.next();
            if (criteriaSeg instanceof ICompareCriteria) {
                ICompareCriteria compCriteria = (ICompareCriteria) criteriaSeg;                   
                expr = compCriteria.getLeftExpression();
                break;                                                                         
            } else if (criteriaSeg instanceof IBaseInCriteria) {
                expr = ((IBaseInCriteria) criteriaSeg).getLeftExpression();
                break;               
            }
        }    
        final String column = "CriteriaDescTable.RequiredDefaultedParam";
        assertTrue("column name mismatch - expected " + column + " returned " + expr, 
        		CriteriaDesc.nameMatch(expr, column));
    }

    public void testStringifyCriteria() {
        String withQuotes = "'foodle doodle'";
        String withoutQuotes = "foodle doodle";
        assertEquals("stringify failed", withoutQuotes, CriteriaDesc.stringifyCriteria(withQuotes));
    }
    
    public void testStringifyCriteriaDoubleQuotes() {
    	String control = "foodle doodle";
    	String test = "\"foodle doodle\"";
    	assertEquals("stringify failed", control, CriteriaDesc.stringifyCriteria(test));
    }
    
    public void testStringifyCriteriaSingleQuote() {
    	String test = "'ello govnor.";
    	assertEquals("stringify failed", test, CriteriaDesc.stringifyCriteria(test));
    }

    public void testStringifyCriteriaSingleDoubleQuote() {
    	String test = "\"ello govnor.";
    	assertEquals("stringify failed", test, CriteriaDesc.stringifyCriteria(test));
    }
    
    
    public void testBadTableSelect() {
    	String tempVdbpath = vdbPath;
    	vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/UnitTests.vdb";
    	try {
    		CriteriaDesc desc = createCriteriaDesc("select BadCol1 from BadTable");
    	} catch (ConnectorException ce) {
    		ce.printStackTrace();
    		fail(ce.getMessage());
    	} finally {
    		vdbPath = tempVdbpath;	
    	}    	    	    	
    }
    
    public void testElementAllowsEmpty() {
    	String tempVdbpath = vdbPath;
    	vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/UnitTests.vdb";
    	String strQuery = "Select Balance from Response";
    	try {
    		Element elem = getElement(strQuery);
    		IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
    		CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, query);
    	} catch (ConnectorException ce) {
    		ce.printStackTrace();
    		fail(ce.getMessage());
    	} finally {
    		vdbPath = tempVdbpath;	
    	}     	
    	
    	
    }
    
    
    private CriteriaDesc createCriteriaDesc(String query) throws ConnectorException {
        Element elem = getElement(query);
        ArrayList list = new ArrayList();
        list.add(VALUE);
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        return desc;
    }

    private Element getElement(String query) throws ConnectorException {
    	return getElement(query, 0);
    }
    
    private Element getElement(String query, int colLocation)
			throws ConnectorException {
        IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	Element elem = ((IElement) expr).getMetadataObject();
    	return elem;        		
	}
    
}
