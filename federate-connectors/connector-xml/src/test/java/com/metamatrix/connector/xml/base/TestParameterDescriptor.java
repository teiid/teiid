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

package com.metamatrix.connector.xml.base;

import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.language.ISelectSymbol;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestParameterDescriptor extends TestCase {

	private static String vdbPath;
    private static final String QUERY = "select OutputColumn from CriteriaDescTable where"
    		+ " OutputColumn in ('MetaMatrix') order by OutputColumn";
 
    
    static {
    	vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
    }
    
    
//  removing hansel while testing clover
/*    
    public static Test suite() {
    	return new CoverageDecorator(ParameterDescriptorTest.class, new Class[] {ParameterDescriptor.class}); 
    }
    
*/    
    /**
     * Constructor for ParameterDescriptorTest.
     * @param arg0
     */
    public TestParameterDescriptor(String arg0) {
        super(arg0);
    }

    /*
     * Class under test for void ParameterDescriptor(Element)
     */
    public void testParameterDescriptorElement() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            ParameterDescriptor desc = new ParameterDescriptorImpl(element);
            assertNotNull(desc);
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    }
    
    
    public void testParameterDescriptorElementParameter() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, "select RequiredDefaultedParam from CriteriaDescTable");
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            ParameterDescriptor desc = new ParameterDescriptorImpl(element);
            assertNotNull(desc);
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    }
    
    
    
    public void testParameterDescriptorElementSpaceXPath() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, "select OutputColumnSpaceXPath from CriteriaDescTable");
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            ParameterDescriptor desc = new ParameterDescriptorImpl(element);
            assertNotNull(desc);
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    }

    /*
     * Class under test for void ParameterDescriptor(ILiteral)
     */
    public void testParameterDescriptor() {
    	ParameterDescriptor desc = new ParameterDescriptorImpl();
    	assertNotNull(desc);
    	assertNull(desc.getXPath());
    	assertFalse(desc.isParameter());
    	assertNull(desc.getColumnName());
    }

    public void testSetGetXPath() {
    	ParameterDescriptor desc = getParameterDescriptor();
    	String xpath = "/foo";
    	desc.setXPath(xpath);
    	assertEquals(xpath, desc.getXPath());
    }

    public void testSetIsParameter() {
    	ParameterDescriptor desc = getParameterDescriptor();
    	boolean is = !desc.isParameter();
    	desc.setIsParameter(is);
    	assertEquals(is, desc.isParameter());
    }

    public void testSetGetColumnName() {
    	ParameterDescriptor desc = getParameterDescriptor();
    	String name = "myColumn";
    	desc.setColumnName(name);
    	assertEquals(name, desc.getColumnName());
    }

    public void testSetGetColumnNumber() {
    	ParameterDescriptor desc = getParameterDescriptor();
    	int number = desc.getColumnNumber() + 1;
    	desc.setColumnNumber(number);
    	assertEquals(number, desc.getColumnNumber());
    }

    public void testGetElement() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            ParameterDescriptor desc = new ParameterDescriptorImpl(element);
            assertEquals(element, desc.getElement());
        } catch (ConnectorException ce) {
        	ce.printStackTrace();
        	fail(ce.getMessage());
        }
    }

    public void testTestForParam() {
    	String trueQuery = "select EmptyCol from EmptyTable where EmptyCol = 'foo'";
    	String falseQuery = "select Company_id from Company";
    	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
    	
    	IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, trueQuery);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
             MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
             element = (Element) metadata.getObject(elementID);
             assertTrue(ParameterDescriptor.testForParam(element));
        } catch (ConnectorException ce) {
        	ce.printStackTrace();
        	fail(ce.getMessage());
        }
        
        query = ProxyObjectFactory.getDefaultIQuery(vdbPath, falseQuery);
        select = query.getSelect();
        symbols = select.getSelectSymbols();
        selectSymbol = (ISelectSymbol) symbols.get(0);
        expr = selectSymbol.getExpression();
        element = null;
        try {
             MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
             element = (Element) metadata.getObject(elementID);
             assertFalse(ParameterDescriptor.testForParam(element));
        } catch (ConnectorException ce) {
        	ce.printStackTrace();
        	fail(ce.getMessage());
        } 
    }
    
    private ParameterDescriptor getParameterDescriptor() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            ParameterDescriptor desc = new ParameterDescriptorImpl(element);
            assertNotNull(desc);
            return desc;
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
            return null;
        }
    }

    private class ParameterDescriptorImpl extends ParameterDescriptor {
    	public ParameterDescriptorImpl() {
    		super();
    	}
    	
    	public ParameterDescriptorImpl(Element element) throws ConnectorException {
    		super(element);
    	}
    	
    }
}
