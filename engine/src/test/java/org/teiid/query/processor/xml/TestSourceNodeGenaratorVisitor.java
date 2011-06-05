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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingBaseNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingException;
import org.teiid.query.mapping.xml.MappingLoader;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.optimizer.xml.SourceNodeGenaratorVisitor;

import junit.framework.TestCase;



/** 
 * 
 */
public class TestSourceNodeGenaratorVisitor extends TestCase {
    
    private MappingDocument loadMappingDocument(String xml) throws MappingException {
        MappingLoader reader = new MappingLoader();

        byte[] bytes = xml.getBytes();

        InputStream istream = new ByteArrayInputStream (bytes);
        
        return reader.loadDocument(istream);
    }    
    
    public void testSourceAtRootXML50() throws Exception{
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>license</name>\r\n" +  //$NON-NLS-1$
            "        <source>licenseSource</source>\r\n" +  //$NON-NLS-1$
            "        <minOccurs>0</minOccurs>\r\n" +  //$NON-NLS-1$
            "        <maxOccurs>unbounded</maxOccurs>\r\n" +  //$NON-NLS-1$
            "        <tempGroup>testTempGroup1</tempGroup>\r\n" +  //$NON-NLS-1$
            "        <tempGroup>testTempGroup2</tempGroup>\r\n" +  //$NON-NLS-1$            
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        // load the original
        MappingDocument doc = loadMappingDocument(xml);

        // now extract the source nodes
        doc = SourceNodeGenaratorVisitor.extractSourceNodes(doc);
        
        // check the staging tables        
        MappingBaseNode root = doc.getRootNode();
        assertTrue(root instanceof MappingSourceNode);
        MappingSourceNode source = (MappingSourceNode)root;
        assertEquals("licenseSource", source.getResultName()); //$NON-NLS-1$
        
        List<String> list = source.getStagingTables();
        assertEquals(2, list.size());

        assertEquals("testTempGroup1", list.get(0)); //$NON-NLS-1$
        assertEquals("testTempGroup2", list.get(1)); //$NON-NLS-1$
        

        MappingBaseNode node = (MappingBaseNode)source.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);
        MappingElement element = (MappingElement)node;
        
        // make sure name matches and caridinality of root is reset; as there can be only one root
        assertEquals("license", element.getName()); //$NON-NLS-1$
        assertEquals(1, element.getMinOccurence());
        assertEquals(1, element.getMaxOccurence());
        assertNull(element.getSource()); 
    }
    
    public void testSourceBelowRootXML50() throws Exception{
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "        <minOccurs>0</minOccurs>\r\n" +  //$NON-NLS-1$
            "        <maxOccurs>unbounded</maxOccurs>\r\n" +  //$NON-NLS-1$
            "        <tempGroup>testTempGroup1</tempGroup>\r\n" +  //$NON-NLS-1$
            "       <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>childNode</name>\r\n" +  //$NON-NLS-1$
            "        <source>childNodeSource</source>\r\n" +  //$NON-NLS-1$
            "        <tempGroup>testTempGroup2</tempGroup>\r\n" +  //$NON-NLS-1$            
            "       </mappingNode>\r\n" +  //$NON-NLS-1$            
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        // load the original
        MappingDocument doc = loadMappingDocument(xml);

        // now extract the source nodes
        doc = SourceNodeGenaratorVisitor.extractSourceNodes(doc);
        
        // check source node
        MappingBaseNode node = doc.getRootNode();
        assertTrue(node instanceof MappingElement);
        MappingElement element = (MappingElement)node;

        List<String> list = element.getStagingTables();
        assertEquals(1, list.size());
        assertEquals("testTempGroup1", list.get(0)); //$NON-NLS-1$
        
        // make sure name matches and caridinality of root is reset; as there can be only one root
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        assertEquals(1, element.getMinOccurence());
        assertEquals(1, element.getMaxOccurence());
        
        MappingNode node1 = element.getNodeChildren().get(0);
        assertTrue(node1 instanceof MappingSourceNode);
        
        MappingSourceNode source = (MappingSourceNode)node1;        
        assertEquals("childNodeSource", source.getResultName()); //$NON-NLS-1$

        list = source.getStagingTables();
        assertEquals(1, list.size());
        assertEquals("testTempGroup2", list.get(0)); //$NON-NLS-1$
        
        // make sure source's child is mapping element and mapping element's source
        // is above source        
        node1 = source.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);
        element = (MappingElement)node1;
        assertEquals("childNode", element.getName()); //$NON-NLS-1$
        assertNull(element.getSource());
    }
    
    
    public void testRecursiveNodeXML50() throws Exception{
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "        <source>parentNodeSource</source>\r\n" +  //$NON-NLS-1$
            "       <mappingNode>\r\n" +  //$NON-NLS-1$
            "           <name>childNode</name>\r\n" +  //$NON-NLS-1$
            "           <mappingNode>\r\n" +  //$NON-NLS-1$
            "               <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$            
            "               <name>attributename</name>\r\n" +  //$NON-NLS-1$
            "               <default>ddd</default>\r\n" +  //$NON-NLS-1$
            "               <fixed>fff</fixed>\r\n" +  //$NON-NLS-1$            
            "           </mappingNode>\r\n" +  //$NON-NLS-1$
            "           <mappingNode>\r\n" +  //$NON-NLS-1$            
            "               <name>recursivenodename</name>\r\n" +  //$NON-NLS-1$
            "               <isRecursive>TRUE</isRecursive>\r\n" +  //$NON-NLS-1$
            "               <recursionLimit>8</recursionLimit>\r\n" +  //$NON-NLS-1$
            "               <recursionCriteria>rrr</recursionCriteria>\r\n" +  //$NON-NLS-1$
            "               <recursionRootMappingClass>parentNodeSource</recursionRootMappingClass>\r\n" +  //$NON-NLS-1$            
            "           </mappingNode>\r\n" +  //$NON-NLS-1$
            "       </mappingNode>\r\n" +  //$NON-NLS-1$            
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        // load the original
        MappingDocument doc = loadMappingDocument(xml);

        // now extract the source nodes
        doc = SourceNodeGenaratorVisitor.extractSourceNodes(doc);
        
        MappingNode node = doc.getRootNode();
        assertTrue(node instanceof MappingSourceNode);
        MappingSourceNode source = (MappingSourceNode)node;
        assertEquals("parentNodeSource", source.getSource()); //$NON-NLS-1$
        
        node = source.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);        
        MappingElement element = (MappingElement)node;
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        assertTrue(element.isRootRecursiveNode());
        assertFalse(element.isRecursive());
        
        node = element.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);        
        element = (MappingElement)node;
        assertEquals("childNode", element.getName()); //$NON-NLS-1$

        List attrs = element.getAttributes();
        assertEquals(1, attrs.size());
        MappingAttribute attribute = (MappingAttribute)attrs.get(0);
        assertEquals("attributename", attribute.getName()); //$NON-NLS-1$
        assertEquals("ddd", attribute.getDefaultValue()); //$NON-NLS-1$
        assertEquals("fff", attribute.getValue()); //$NON-NLS-1$
        
        node = element.getNodeChildren().get(0);
        assertTrue(node instanceof MappingRecursiveElement);        
        MappingRecursiveElement recursive = (MappingRecursiveElement)node;
        assertEquals("recursivenodename", recursive.getName()); //$NON-NLS-1$
        assertEquals(8, recursive.getRecursionLimit());
        assertFalse(recursive.isRootRecursiveNode());
        assertTrue(recursive.isRecursive());
    }
    
    public void testRecursiveElementXML50() throws Exception{
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "   <mappingNode>\r\n" +  //$NON-NLS-1$
            "       <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <source>parentSource</source>\r\n" +  //$NON-NLS-1$
            "       <mappingNode>"+ //$NON-NLS-1$
            "           <name>childNode</name>\r\n" +  //$NON-NLS-1$   
            "           <source>childSource</source>\r\n" +  //$NON-NLS-1$
            "           <recursionRootMappingClass>parentSource</recursionRootMappingClass>" + //$NON-NLS-1$
            "           <isRecursive>true</isRecursive>"+ //$NON-NLS-1$
            "           <recursionLimit>6</recursionLimit>" + //$NON-NLS-1$
            "           <recursionCriteria>foo</recursionCriteria>" + //$NON-NLS-1$
            "       </mappingNode>"+ //$NON-NLS-1$                
            "   </mappingNode>\r\n" +  //$NON-NLS-1$                
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
         
        // load the original
        MappingDocument doc = loadMappingDocument(xml);

        // now extract the source nodes
        doc = SourceNodeGenaratorVisitor.extractSourceNodes(doc);
            
        MappingNode node = doc.getRootNode();
        assertTrue(node instanceof MappingSourceNode);
        
        // ist source
        MappingSourceNode source = (MappingSourceNode)node;
        assertEquals("parentSource", source.getSource()); //$NON-NLS-1$
        
        // parent element
        node = source.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);        
        MappingElement element = (MappingElement)node;
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        assertTrue(element.isRootRecursiveNode());
        assertFalse(element.isRecursive());
        
        // recursive source
        node = element.getNodeChildren().get(0);
        source = (MappingSourceNode)node;
        assertEquals("childSource", source.getSource()); //$NON-NLS-1$
        assertEquals("parentSource", source.getAliasResultName()); //$NON-NLS-1$
        
        node = source.getNodeChildren().get(0);
        assertTrue(node instanceof MappingRecursiveElement);        
        MappingRecursiveElement relement = (MappingRecursiveElement)node;
        
        assertTrue(relement.isRecursive());
        assertEquals("childNode", relement.getName()); //$NON-NLS-1$
        assertEquals("foo", relement.getCriteria()); //$NON-NLS-1$
        assertEquals(6, relement.getRecursionLimit());
    }
}
