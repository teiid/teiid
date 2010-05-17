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

package org.teiid.translator.xml;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.language.BaseInCondition;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.LanguageUtil;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.CriteriaDesc;


/**
 *
 */
public class TestCriteriaDesc extends TestCase {
    
    private static String vdbPath;
    private static final String QUERY = "select RequiredDefaultedParam from CriteriaDescTable "  //$NON-NLS-1$
    		+ "where RequiredDefaultedParam in ('foo') order by RequiredDefaultedParam"; //$NON-NLS-1$
    private static final String VALUE = "value1"; //$NON-NLS-1$
   
    
    
    static {
    	vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
    }
    
    public void testGetCriteriaDescForColumn() throws Exception {  
    	//case 1: values provided
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam in ('foo')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }

    public void testGetCriteriaDescForColumnDefaultedValue() throws Exception {
    	//case 2: param, required, defaulted
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnNoCriteria() throws Exception {
    	//case 3: param, not required, not defaulted, not allowed empty
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNull("CriteriaDesc is not null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnAllowEmpty() throws Exception {
    	//case 4: param, not required, not defaulted, allowed empty
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select OptionalAllowedEmptyParam from CriteriaDescTable"; //$NON-NLS-1$
    	final int colLocation = 0;
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnError() {
    	//case 5: param, required, not defaulted
        try {
        	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
        	String query = "select RequiredUndefaultedParam from CriteriaDescTable"; //$NON-NLS-1$
        	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
        	Expression expr = symbol.getExpression();
        	Column elem = ((ColumnReference) expr).getMetadataObject();
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("exception not thrown"); //$NON-NLS-1$
        } catch (TranslatorException ce) {
        }   	
    }
    
    public void testGetCriteriaDescForColumnNotParam() throws Exception {
    	//case 6: not a param
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select OutputColumn from CriteriaDescTable"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNull("CriteriaDesc is not null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnCompare() throws Exception {  
    	//case 7: compare criteria
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam = 'foo'"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    
    public void testGetCriteriaDescForColumnMultiElement() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select MultiElementParam from CriteriaDescTable where MultiElementParam in ('foo','bar')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
    	String multiplicityStr = elem.getProperties().get(
				CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnDelimited() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo','bar')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
    	String multiplicityStr = elem.getProperties().get(
    			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnLikeSearchable() {  
        try {
        	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
        	String query = "select LikeSearchableParam from CriteriaDescTable where LikeSearchableParam in ('foo')"; //$NON-NLS-1$
        	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
        	Expression expr = symbol.getExpression();
        	Column elem = ((ColumnReference) expr).getMetadataObject();
        	String multiplicityStr = elem.getProperties().get(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
            CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("should not be able to handle default value"); //$NON-NLS-1$
        } catch (TranslatorException ce) {
        }        
    }
    
    public void testGetCriteriaDescForColumnUnlikeSearchable() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select UnlikeSearchableParam from CriteriaDescTable where UnlikeSearchableParam in ('foo')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
    	String multiplicityStr = elem.getProperties().get(
    			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
    }
    
    public void testGetCriteriaDescForColumnUnsearchable() {  
        try {
        	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
        	String query = "select UnsearchableParam from CriteriaDescTable where UnsearchableParam in ('foo','bar')"; //$NON-NLS-1$
        	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
        	Expression expr = symbol.getExpression();
        	Column elem = ((ColumnReference) expr).getMetadataObject();
        	String multiplicityStr = elem.getProperties().get(
        			CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
            CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
            fail("should not be able to handle default value"); //$NON-NLS-1$
        } catch (TranslatorException ce) {
        }        
    }
    
    public void testGetCriteriaDescForColumnLike() throws Exception {  
    	//case 1: values provided
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam like 'foo'"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }

    public void testGetCriteriaDescForColumnNotEquals() throws Exception {  
    	//case 1: values provided
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam != 'foo'"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where concat(RequiredDefaultedParam, 'bar') in('foo')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnNameMatchFailure() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where AttributeParam in('foo')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    
    public void testGetCriteriaDescForColumnLeftLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable "  //$NON-NLS-1$
    			+ "where concat('bar', 'foo') = concat('bar', RequiredDefaultedParam)"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnTwoElements() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where OutputColumn = RequiredDefaultedParam"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnLeftElementEqualsLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where AttributeParam = 'foo'"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetCriteriaDescForColumnLeftElementEqualsNonLiteral() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select RequiredDefaultedParam from CriteriaDescTable where"  //$NON-NLS-1$
    			+ " RequiredDefaultedParam = concat('foo', OutputColumn)"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertNotNull("CriteriaDesc is null", desc); //$NON-NLS-1$
    }
    
    public void testGetInputXPathNoXpath() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select BadNoInputXpath from CriteriaDescTable where BadNoInputXpath in ('foo')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals(desc.getColumnName(), desc.getInputXpath());
    }
    
    
    public void testGetInputXPathEmptyXpath() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select BadEmptyInputXPath from CriteriaDescTable where BadEmptyInputXPath in ('foo')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals(desc.getColumnName(), desc.getInputXpath());
    }  
    
    public void testGetDataAttributeNameEmptyName() throws Exception {  
    	assertNotNull("vdb path is null", vdbPath); //$NON-NLS-1$
    	String query = "select BadNoDataAttributeName from CriteriaDescTable where BadNoDataAttributeName in ('foo')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals("", desc.getDataAttributeName()); //$NON-NLS-1$
    } 
    
    public void testGetInputXpath() throws TranslatorException {
    	String query = "select RequiredDefaultedParam from CriteriaDescTable"; //$NON-NLS-1$
    	String inputXPath = "/req/default/value"; //$NON-NLS-1$
        CriteriaDesc desc = createCriteriaDesc(query);
        assertNotNull("CriteriaDesc is null", desc.getInputXpath()); //$NON-NLS-1$
        assertEquals(inputXPath, desc.getInputXpath());
    }

    public void testIsUnlimited() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        assertFalse("Criteria is flagged as unlimited", desc.isUnlimited());             //$NON-NLS-1$
    }

    public void testIsAutoIncrement() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        assertFalse("criterion is flagged as autoIncrement", desc.isAutoIncrement()); //$NON-NLS-1$
    }

    public void testIsParentAttribute() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        //before its inited
        assertFalse("criterion is flagged as an attribute", desc.isParentAttribute()); //$NON-NLS-1$
        //and after for code coverage
        assertFalse("criterion is flagged as an attribute", desc.isParentAttribute()); //$NON-NLS-1$
    }

    public void testIsEnumeratedAttribute() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        assertFalse("criterion is an enumerated attribute", desc.isEnumeratedAttribute()); //$NON-NLS-1$
    }

    public void testAllowEmptyValueFalse() throws TranslatorException {
    	String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable"; //$NON-NLS-1$
        CriteriaDesc desc = createCriteriaDesc(query);
        assertFalse("criterion should not allow for empty values", desc.allowEmptyValue()); //$NON-NLS-1$
    }
    
    public void testAllowEmptyValueTrue() throws TranslatorException {
    	String query = "select OptionalAllowedEmptyParam from CriteriaDescTable"; //$NON-NLS-1$
        CriteriaDesc desc = createCriteriaDesc(query);
        //before init
        assertTrue("criterion should allow for empty values", desc.allowEmptyValue()); //$NON-NLS-1$
        //and after for code coverage
        assertTrue("criterion should allow for empty values", desc.allowEmptyValue()); //$NON-NLS-1$
    }

    public void testIsDataInAttributeFalse() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        assertFalse("criterion is flagged as data in attribute", desc.isDataInAttribute()); //$NON-NLS-1$
    }
    
    public void testIsDataInAttributeTrue() throws TranslatorException {
    	String query = "select AttributeParam from CriteriaDescTable where AttributeParam in ('foo')"; //$NON-NLS-1$
        CriteriaDesc desc = createCriteriaDesc(query);
        assertTrue("criterion is not flagged as data in attribute", desc.isDataInAttribute()); //$NON-NLS-1$
    }

    public void testGetDataAttributeName() throws TranslatorException {
    	String query = "select AttributeColumn from TestTable where AttributeColumn in ('foo')"; //$NON-NLS-1$
        Column elem = getElement(query);
        String attributeName = "myAttribute"; //$NON-NLS-1$
        ArrayList list = new ArrayList();
        list.add(VALUE);
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        assertNotNull("CriteriaDesc is null", desc.getDataAttributeName()); //$NON-NLS-1$
        assertTrue("column name mismatch - expected " + attributeName  //$NON-NLS-1$
        		 + " returned " + desc.getDataAttributeName(),  //$NON-NLS-1$
				desc.getDataAttributeName().equals(attributeName));            
    }

    public void testGetValues() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        List values = desc.getValues();
        assertNotNull("Values list is null", values); //$NON-NLS-1$
        assertEquals(values.get(0), VALUE);
    }

    public void testGetNumberOfValues() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        assertEquals(1, desc.getNumberOfValues());
    }

    public void testGetCurrentIndexValue() throws TranslatorException {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        assertEquals(VALUE, desc.getCurrentIndexValue());
    }
    
    public void testGetCurrentIndexValueEnumerated() throws Exception {
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo', 'bar')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertEquals("There should be two values" , 2, desc.getNumberOfValues()); //$NON-NLS-1$
        assertEquals("foo", desc.getCurrentIndexValue()); //$NON-NLS-1$
        desc.incrementIndex();
        assertEquals("bar", desc.getCurrentIndexValue()); //$NON-NLS-1$
    }
    
    public void testIncrementIndexEnumerated() throws Exception {
    	String query = "select DelimitedParam from CriteriaDescTable where DelimitedParam in ('foo', 'bar')"; //$NON-NLS-1$
    	Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	final int colLocation = 0;
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
    	Column elem = ((ColumnReference) expr).getMetadataObject();
        CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, iquery);
        assertTrue("We should be able to increment this CriteriaDesc", desc.incrementIndex()); //$NON-NLS-1$
    }
    
    public void testGetCurrentIndexValueNoValue() throws Exception {
    	final String query = "select OptionalAllowedEmptyParam from CriteriaDescTable"; //$NON-NLS-1$
        Column elem = getElement(query);
        ArrayList list = new ArrayList();
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        assertEquals("", desc.getCurrentIndexValue()); //$NON-NLS-1$
    }

    public void testGetCurrentIndexValueNoValueNotEmpty() throws Exception {
    	final String query = "select OptionalNotAllowedEmptyParam from CriteriaDescTable"; //$NON-NLS-1$
        Column elem = getElement(query);
        ArrayList list = new ArrayList();
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        assertNull(desc.getCurrentIndexValue());
    }
    
    public void testIncrementIndex() throws Exception {
        final String value2 = "value2"; //$NON-NLS-1$
        String query = "select MultiCol from MultiTable where MultiCol in ('" + VALUE + "', '" + value2 + "')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Column elem = getElement(query);
        ArrayList list = new ArrayList();
        list.add(VALUE);
        list.add(value2);           
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        assertEquals(VALUE, desc.getCurrentIndexValue());
        assertTrue("index increment failed", desc.incrementIndex()); //$NON-NLS-1$
        assertEquals(value2, desc.getCurrentIndexValue());
        assertFalse("index went beyond number of values", desc.incrementIndex()); //$NON-NLS-1$
    }

    public void testResetIndex() throws Exception {
        CriteriaDesc desc = createCriteriaDesc(QUERY);
        desc.resetIndex();
    }

    public void testNameMatch() {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        Condition crit = query.getWhere();
        List criteriaList = LanguageUtil.separateCriteriaByAnd(crit);        
        Iterator criteriaIter = criteriaList.iterator();
        Expression expr = null;
        while (criteriaIter.hasNext()) {
            Condition criteriaSeg = (Condition) criteriaIter.next();
            if (criteriaSeg instanceof Comparison) {
                Comparison compCriteria = (Comparison) criteriaSeg;                   
                expr = compCriteria.getLeftExpression();
                break;                                                                         
            } else if (criteriaSeg instanceof BaseInCondition) {
                expr = ((BaseInCondition) criteriaSeg).getLeftExpression();
                break;               
            }
        }    
        final String column = "CriteriaDescTable.RequiredDefaultedParam"; //$NON-NLS-1$
        assertTrue("column name mismatch - expected " + column + " returned " + expr,  //$NON-NLS-1$ //$NON-NLS-2$
        		CriteriaDesc.nameMatch(expr, column));
    }

    public void testStringifyCriteria() {
        String withQuotes = "'foodle doodle'"; //$NON-NLS-1$
        String withoutQuotes = "foodle doodle"; //$NON-NLS-1$
        assertEquals("stringify failed", withoutQuotes, CriteriaDesc.stringifyCriteria(withQuotes)); //$NON-NLS-1$
    }
    
    public void testStringifyCriteriaDoubleQuotes() {
    	String control = "foodle doodle"; //$NON-NLS-1$
    	String test = "\"foodle doodle\""; //$NON-NLS-1$
    	assertEquals("stringify failed", control, CriteriaDesc.stringifyCriteria(test)); //$NON-NLS-1$
    }
    
    public void testStringifyCriteriaSingleQuote() {
    	String test = "'ello govnor."; //$NON-NLS-1$
    	assertEquals("stringify failed", test, CriteriaDesc.stringifyCriteria(test)); //$NON-NLS-1$
    }

    public void testStringifyCriteriaSingleDoubleQuote() {
    	String test = "\"ello govnor."; //$NON-NLS-1$
    	assertEquals("stringify failed", test, CriteriaDesc.stringifyCriteria(test)); //$NON-NLS-1$
    }
    
    
    public void testBadTableSelect() throws TranslatorException {
    	String tempVdbpath = vdbPath;
    	vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/UnitTests.vdb"; //$NON-NLS-1$
    	try {
    		createCriteriaDesc("select BadCol1 from BadTable"); //$NON-NLS-1$
    	} finally {
    		vdbPath = tempVdbpath;	
    	}    	    	    	
    }
    
    public void testElementAllowsEmpty() throws TranslatorException {
    	String tempVdbpath = vdbPath;
    	vdbPath = ProxyObjectFactory.getDocumentsFolder() + "/UnitTests.vdb"; //$NON-NLS-1$
    	String strQuery = "Select Balance from Response"; //$NON-NLS-1$
    	try {
    		Column elem = getElement(strQuery);
    		Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
    		CriteriaDesc desc = CriteriaDesc.getCriteriaDescForColumn(elem, query);
    	} finally {
    		vdbPath = tempVdbpath;	
    	}     	
    }
    
    
    private CriteriaDesc createCriteriaDesc(String query) throws TranslatorException {
        Column elem = getElement(query);
        ArrayList list = new ArrayList();
        list.add(VALUE);
        CriteriaDesc desc = new CriteriaDesc(elem, list);
        return desc;
    }

    private Column getElement(String query) throws TranslatorException {
    	return getElement(query, 0);
    }
    
    private Column getElement(String query, int colLocation)
			throws TranslatorException {
        Select iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	DerivedColumn symbol = iquery.getDerivedColumns().get(colLocation);
    	Expression expr = symbol.getExpression();
    	Column elem = ((ColumnReference) expr).getMetadataObject();
    	return elem;        		
	}
    
}
