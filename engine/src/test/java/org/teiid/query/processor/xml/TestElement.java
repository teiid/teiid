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

package org.teiid.query.processor.xml;

import java.io.CharArrayWriter;
import java.util.Properties;

import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.processor.xml.Element;
import org.teiid.query.processor.xml.NodeDescriptor;

import junit.framework.TestCase;
import net.sf.saxon.TransformerFactoryImpl;


public class TestElement  extends TestCase{
	private CharArrayWriter streamResultHolder;
	private TransformerHandler handler;
    
	public TestElement(String name) {
		super(name);
	}

    public void setUp() throws Exception{
    	streamResultHolder = new CharArrayWriter();	
        SAXTransformerFactory factory = new TransformerFactoryImpl();
		handler = factory.newTransformerHandler();
		handler.setResult(new StreamResult(streamResultHolder));
		handler.startDocument();
    }

    public void testStartAndEndEmptyElement() throws Exception{
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("E1", null, true, null, null,  null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
    	Element element = new Element(descriptor, handler); 
    	element.startElement();
    	element.endElement();
    	handler.endDocument();
    	assertEquals(new String(streamResultHolder.toCharArray()), "<?xml version=\"1.0\" encoding=\"UTF-8\"?><E1/>"); //$NON-NLS-1$
    }
    
    public void testStartAndEndElement() throws Exception{
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("E1", null, true, null, null, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        Element element = new Element(descriptor, handler); 
        element.setContent("test"); //$NON-NLS-1$
    	element.startElement();
    	element.endElement();
    	handler.endDocument();
    	assertEquals(new String(streamResultHolder.toCharArray()), "<?xml version=\"1.0\" encoding=\"UTF-8\"?><E1>test</E1>");//$NON-NLS-1$
    }
    
    public void testAddAttributes() throws Exception{
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("E1", null, true, null, null,  null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        Element element = new Element(descriptor, handler); 
        descriptor = NodeDescriptor.createNodeDescriptor("a1", null, true, null, null, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        element.setAttribute(descriptor, "test attribute");  //$NON-NLS-1$
    	element.setContent("test"); //$NON-NLS-1$
    	element.startElement();
    	element.endElement();
    	handler.endDocument();
    	assertEquals(new String(streamResultHolder.toCharArray()), "<?xml version=\"1.0\" encoding=\"UTF-8\"?><E1 a1=\"test attribute\">test</E1>");//$NON-NLS-1$
    }
    
    public void testIsChildOf()throws Exception{
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("E1", null, true, null, null, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        Element element1 = new Element(descriptor, handler); 
        descriptor = NodeDescriptor.createNodeDescriptor("E2", null, true, null, null, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        Element element2 = new Element(descriptor, handler);
        descriptor = NodeDescriptor.createNodeDescriptor("E3", null, true, null, null, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        Element element3 = new Element(descriptor, handler);
        element2.setParent(element1);
    	element3.setParent(element2);
    	assertTrue(element3.isChildOf(element1));
    }
    
    public void testNamespace()throws Exception{
        Properties namespaceURIs = new Properties();
        namespaceURIs.setProperty("n", "http://test");//$NON-NLS-1$ //$NON-NLS-2$  
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("E1", "n", true, null, namespaceURIs, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$//$NON-NLS-2$ 
        Element element = new Element(descriptor, handler); 
    	element.setContent("test"); //$NON-NLS-1$
    	element.startElement();
    	element.endElement();
    	handler.endDocument();
    	assertEquals(new String(streamResultHolder.toCharArray()), "<?xml version=\"1.0\" encoding=\"UTF-8\"?><n:E1 xmlns:n=\"http://test\">test</n:E1>");//$NON-NLS-1$
    }
    
    public void testNamespace2()throws Exception{
        Properties namespaceURIs = new Properties();
        namespaceURIs.setProperty("n", "http://test");//$NON-NLS-1$ //$NON-NLS-2$  
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("E1", null, true, null, namespaceURIs, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        Element element = new Element(descriptor, handler); 
        element.setContent("test"); //$NON-NLS-1$
    	element.startElement();
        namespaceURIs = new Properties();
        namespaceURIs.setProperty("n", "");//$NON-NLS-1$ //$NON-NLS-2$  
        descriptor = NodeDescriptor.createNodeDescriptor("E2", "n", true, null, namespaceURIs, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$//$NON-NLS-2$ 
    	Element element2 = new Element(descriptor, handler);  
    	element2.setContent("test"); //$NON-NLS-1$
    	element2.startElement();
    	element2.endElement();
    	element.endElement();
    	handler.endDocument();
    	//System.out.println(streamResultHolder.toCharArray());
    	assertEquals(new String(streamResultHolder.toCharArray()), "<?xml version=\"1.0\" encoding=\"UTF-8\"?><E1 xmlns:n=\"http://test\">test<n:E2>test</n:E2></E1>");//$NON-NLS-1$
    }
}
