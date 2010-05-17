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

import java.util.List;

import junit.framework.TestCase;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.ParameterDescriptor;


/**
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
    public void testParameterDescriptorElement() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        ParameterDescriptor desc = new ParameterDescriptorImpl(element);
        assertNotNull(desc);
    }
    
    
    public void testParameterDescriptorElementParameter() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, "select RequiredDefaultedParam from CriteriaDescTable");
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        ParameterDescriptor desc = new ParameterDescriptorImpl(element);
        assertNotNull(desc);
    }
    
    
    
    public void testParameterDescriptorElementSpaceXPath() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, "select OutputColumnSpaceXPath from CriteriaDescTable");
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        ParameterDescriptor desc = new ParameterDescriptorImpl(element);
        assertNotNull(desc);
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

    public void testSetGetXPath() throws Exception {
    	ParameterDescriptor desc = getParameterDescriptor();
    	String xpath = "/foo";
    	desc.setXPath(xpath);
    	assertEquals(xpath, desc.getXPath());
    }

    public void testSetIsParameter() throws Exception {
    	ParameterDescriptor desc = getParameterDescriptor();
    	boolean is = !desc.isParameter();
    	desc.setIsParameter(is);
    	assertEquals(is, desc.isParameter());
    }

    public void testSetGetColumnName() throws Exception {
    	ParameterDescriptor desc = getParameterDescriptor();
    	String name = "myColumn";
    	desc.setColumnName(name);
    	assertEquals(name, desc.getColumnName());
    }

    public void testSetGetColumnNumber() throws Exception {
    	ParameterDescriptor desc = getParameterDescriptor();
    	int number = desc.getColumnNumber() + 1;
    	desc.setColumnNumber(number);
    	assertEquals(number, desc.getColumnNumber());
    }

    public void testGetElement() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject(); 
        ParameterDescriptor desc = new ParameterDescriptorImpl(element);
        assertEquals(element, desc.getElement());
    }

    public void testTestForParam() throws Exception {
    	String trueQuery = "select EmptyCol from EmptyTable where EmptyCol = 'foo'";
    	String falseQuery = "select Company_id from Company";
    	
    	Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, trueQuery);
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject();
        assertTrue(ParameterDescriptor.testForParam(element));
        
        query = ProxyObjectFactory.getDefaultIQuery(vdbPath, falseQuery);
        symbols = query.getDerivedColumns();
        selectSymbol = (DerivedColumn) symbols.get(0);
        expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	element = ((ColumnReference) expr).getMetadataObject();
        assertFalse(ParameterDescriptor.testForParam(element));
    }
    
    private ParameterDescriptor getParameterDescriptor() throws Exception {
        Select query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        List symbols = query.getDerivedColumns();
        DerivedColumn selectSymbol = (DerivedColumn) symbols.get(0);
        Expression expr = selectSymbol.getExpression();
    	assertTrue(expr instanceof ColumnReference);
    	Column element = ((ColumnReference) expr).getMetadataObject();        
        ParameterDescriptor desc = new ParameterDescriptorImpl(element);
        assertNotNull(desc);
        return desc;
    }

    private class ParameterDescriptorImpl extends ParameterDescriptor {
    	public ParameterDescriptorImpl() {
    		super();
    	}
    	
    	public ParameterDescriptorImpl(Column element) throws TranslatorException {
    		super(element);
    	}
    	
    }
}
