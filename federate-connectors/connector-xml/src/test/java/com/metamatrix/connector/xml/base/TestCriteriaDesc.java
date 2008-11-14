/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;


import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IBaseInCriteria;
import com.metamatrix.data.language.ICompareCriteria;
import com.metamatrix.data.language.ICriteria;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFrom;
import com.metamatrix.data.language.IGroup;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.language.ISelectSymbol;
import com.metamatrix.data.language.LanguageUtil;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.Group;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * created by JChoate on Jun 27, 2005
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
    	
    }
    
    /**
     * Constructor for CriteriaDescTest.
     * @param arg0
     */
    public TestCriteriaDesc(String arg0) {
        super(arg0);
    }

    public void testGetCriteriaDescForColumn() {  
    	//case 1: values provided
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        
    }

    public void testGetCriteriaDescForColumnDefaultedValue() {
    	//case 2: param, required, defaulted
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }   	
    }
    
    public void testGetCriteriaDescForColumnNoCriteria() {
    	//case 3: param, not required, not defaulted, not allowed empty
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNull("CriteriaDesc is not null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }   	
    }
    
    public void testGetCriteriaDescForColumnAllowEmpty() {
    	//case 4: param, not required, not defaulted, allowed empty
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select OptionalAllowedEmptyParam from CriteriaDescTable";
        	final int colLocation = 0;
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }   	
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
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("exception not thrown");
        } catch (ConnectorException ce) {
            assertNotNull(ce);
        }   	
    }
    
    public void testGetCriteriaDescForColumnNotParam() {
    	//case 6: not a param
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select OutputColumn from CriteriaDescTable";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNull("CriteriaDesc is not null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }   	
    }
    
    public void testGetCriteriaDescForColumnCompare() {  
    	//case 7: compare criteria
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam = 'foo'";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        
    }
    
    
    public void testGetCriteriaDescForColumnMultiElement() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select MultiElementParam from CriteriaDescTable where MultiElementParam in ('foo','bar')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	String multiplicityStr = elem.getProperties().getProperty(
    				CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        	System.out.println(multiplicityStr);
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testGetCriteriaDescForColumnDelimited() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo','bar')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	String multiplicityStr = elem.getProperties().getProperty(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        	System.out.println(multiplicityStr);
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testGetCriteriaDescForColumnLikeSearchable() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select LikeSearchableParam from CriteriaDescTable where LikeSearchableParam in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	String multiplicityStr = elem.getProperties().getProperty(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        	System.out.println(multiplicityStr);
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("should not be able to handle default value");
        } catch (ConnectorException ce) {
            assertTrue(true);
        }        
    }
    
    public void testGetCriteriaDescForColumnUnlikeSearchable() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select UnlikeSearchableParam from CriteriaDescTable where UnlikeSearchableParam in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	String multiplicityStr = elem.getProperties().getProperty(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        	System.out.println(multiplicityStr);
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("could not create CriteriaDesc", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testGetCriteriaDescForColumnUnsearchable() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select UnsearchableParam from CriteriaDescTable where UnsearchableParam in ('foo','bar')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	String multiplicityStr = elem.getProperties().getProperty(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        	System.out.println(multiplicityStr);
            CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("should not be able to handle default value");
        } catch (ConnectorException ce) {
            assertTrue(true);
        }        
    }
    
    public void testGetCriteriaDescForColumnLike() {  
    	//case 1: values provided
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam like 'foo'";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        
    }

    public void testGetCriteriaDescForColumnNotEquals() {  
    	//case 1: values provided
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam != 'foo'";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        
    }
    
    public void testGetCriteriaDescForColumnLiteral() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where concat(RequiredDefaultedParam, 'bar') in('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        
    }
    
    public void testGetCriteriaDescForColumnNameMatchFailure() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where AttributeParam in('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
        
    }
    
    
    public void testGetCriteriaDescForColumnLeftLiteral() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable " 
        			+ "where concat('bar', 'foo') = concat('bar', RequiredDefaultedParam)";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testGetCriteriaDescForColumnTwoElements() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where OutputColumn = RequiredDefaultedParam";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testGetCriteriaDescForColumnLeftElementEqualsLiteral() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where AttributeParam = 'foo'";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testGetCriteriaDescForColumnLeftElementEqualsNonLiteral() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where" 
        			+ " RequiredDefaultedParam = concat('foo', OutputColumn)";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertNotNull("CriteriaDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    public void testGetInputXPathNoXpath() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select BadNoInputXpath from CriteriaDescTable where BadNoInputXpath in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertEquals(desc.getColumnName(), desc.getInputXpath());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }
    
    
    public void testGetInputXPathEmptyXpath() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select BadEmptyInputXPath from CriteriaDescTable where BadEmptyInputXPath in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertEquals(desc.getColumnName(), desc.getInputXpath());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
    }  
    
    public void testGetDataAttributeNameEmptyName() {  
        try {
        	assertNotNull("vdb path is null", vdbPath);
        	String query = "select BadNoDataAttributeName from CriteriaDescTable where BadNoDataAttributeName in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
        	System.out.println(symbol.toString());
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            assertEquals("", desc.getDataAttributeName());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }        
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
    
    public void testGetCurrentIndexValueEnumerated() {
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo', 'bar')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	try {
	    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
	    	IExpression expr = symbol.getExpression();
	    	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
	    	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
	    	Element elem = (Element) metadata.getObject(elementID);
	    	System.out.println(symbol.toString());
	        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
	        System.out.println(desc.getCurrentIndexValue());
	        assertEquals("There should be two values" , 2, desc.getNumberOfValues());
	        assertEquals("foo", desc.getCurrentIndexValue());
	        desc.incrementIndex();
	        assertEquals("bar", desc.getCurrentIndexValue());
    	} catch (ConnectorException ce) {
    		ce.printStackTrace();
    		fail(ce.getMessage());
    	}
    }
    
    public void testIncrementIndexEnumerated() {
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo', 'bar')";
    	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	try {
	    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
	    	IExpression expr = symbol.getExpression();
	    	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
	    	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
	    	Element elem = (Element) metadata.getObject(elementID);
	        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
	        assertTrue("We should be able to increment this CriteriaDesc", desc.incrementIndex());
    	} catch (ConnectorException ce) {
    		ce.printStackTrace();
    		fail(ce.getMessage());
    	}
    }
    
    public void testGetCurrentIndexValueNoValue() {
        try {
        	final String query = "select OptionalAllowedEmptyParam from CriteriaDescTable";
            Element elem = getElement(query);
            ArrayList list = new ArrayList();
            CriteriaDesc desc = new CriteriaDesc(elem, list);
            System.out.println(desc.getCurrentIndexValue());
            assertEquals("", desc.getCurrentIndexValue());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }    	
    }

    public void testGetCurrentIndexValueNoValueNotEmpty() {
        try {
        	final String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable";
            Element elem = getElement(query);
            ArrayList list = new ArrayList();
            CriteriaDesc desc = new CriteriaDesc(elem, list);
            System.out.println(desc.getCurrentIndexValue());
            assertNull(desc.getCurrentIndexValue());
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }    	
    }
    
    public void testIncrementIndex() {
        try {
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
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    public void testResetIndex() {
        try {
            CriteriaDesc desc = createCriteriaDesc(QUERY);
            desc.resetIndex();
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
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
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
    	Element elem = (Element) metadata.getObject(elementID);
    	return elem;        		
	}
    
    private Group getTable(String query) throws ConnectorException {
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        ISelect select = iquery.getSelect();
        List elems = select.getSelectSymbols();
        IFrom from = iquery.getFrom();
        List fromItems = from.getItems();
        //better be only one
        IGroup group = (IGroup) fromItems.get(0);
        MetadataID id = group.getMetadataID();
        return (Group) metadata.getObject(id);
    }
    
    private void checkPath() {
    	File foo = new File(".");
    	System.out.println("using path: " + foo.getAbsolutePath());
    }
    
    
}
