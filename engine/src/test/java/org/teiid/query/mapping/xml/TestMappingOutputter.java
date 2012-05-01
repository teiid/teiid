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

package org.teiid.query.mapping.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingChoiceNode;
import org.teiid.query.mapping.xml.MappingCriteriaNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingException;
import org.teiid.query.mapping.xml.MappingLoader;
import org.teiid.query.mapping.xml.MappingOutputter;
import org.teiid.query.mapping.xml.MappingRecursiveElement;

import junit.framework.TestCase;

/**
 * <p>Test cases for {@link MappingOutputter} class. </p>
 */
public class TestMappingOutputter extends TestCase {
    
    private MappingDocument loadMappingDocument(String xml) 
        throws MappingException {
        MappingLoader reader = new MappingLoader();
        byte[] bytes = xml.getBytes();
        InputStream istream = new ByteArrayInputStream(bytes);
        return reader.loadDocument(istream);
    }    
    
    private String saveMappingDocument(MappingDocument doc) 
        throws IOException {
        StringWriter sw = new StringWriter();
        MappingOutputter out = new MappingOutputter();
        out.write(doc, new PrintWriter(sw));
        return sw.toString();
    }
    
    public void testSourceAtRoot() throws Exception{
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>true</formattedDocument>" +  //$NON-NLS-1$                                    
                "<mappingNode>" +  //$NON-NLS-1$            
                   "<name>license</name>" +  //$NON-NLS-1$
                   "<minOccurs>0</minOccurs>" +  //$NON-NLS-1$
                   "<maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                   "<source>licenseSource</source>" +  //$NON-NLS-1$
                   "<tempGroup>testTempGroup1</tempGroup>" +  //$NON-NLS-1$
                   "<tempGroup>testTempGroup2</tempGroup>" +  //$NON-NLS-1$                                           
                "</mappingNode>" +  //$NON-NLS-1$            
            "</xmlMapping>"; //$NON-NLS-1$
        
        ArrayList stagingTables = new ArrayList();
        stagingTables.add("testTempGroup1"); //$NON-NLS-1$
        stagingTables.add("testTempGroup2"); //$NON-NLS-1$
        
        MappingDocument doc = new MappingDocument("UTF-8", true); //$NON-NLS-1$
        
        MappingElement element = doc.addChildElement(new MappingElement("license")); //$NON-NLS-1$
        element.setStagingTables(stagingTables);
        element.setSource("licenseSource"); //$NON-NLS-1$
        element.setMinOccurrs(0);
        element.setMaxOccurrs(-1);
        
        String savedXML = saveMappingDocument(doc);
        
        assertEquals(expected, savedXML);
        
    }
    
    public void testSourceBelowRoot() throws Exception{
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>true</formattedDocument>" +  //$NON-NLS-1$                                                
                "<mappingNode>" +  //$NON-NLS-1$
                    "<name>parentNode</name>" +  //$NON-NLS-1$
                    "<minOccurs>0</minOccurs>" +  //$NON-NLS-1$
                    "<maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                    "<tempGroup>testTempGroup1</tempGroup>" +  //$NON-NLS-1$
                    "<tempGroup>testTempGroup2</tempGroup>" +  //$NON-NLS-1$                                
                    "<mappingNode>" +  //$NON-NLS-1$
                        "<name>childNode</name>" +  //$NON-NLS-1$
                        "<source>childNodeSource</source>" +  //$NON-NLS-1$
                    "</mappingNode>" +  //$NON-NLS-1$
                "</mappingNode>" +  //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        
        ArrayList stagingTables = new ArrayList();
        stagingTables.add("testTempGroup1"); //$NON-NLS-1$
        stagingTables.add("testTempGroup2"); //$NON-NLS-1$
        
        MappingDocument doc = new MappingDocument("UTF-8", true); //$NON-NLS-1$
        
        MappingElement element = doc.addChildElement(new MappingElement("parentNode")); //$NON-NLS-1$
        element.setMinOccurrs(0);
        element.setMaxOccurrs(-1);
        element.setStagingTables(stagingTables);

        MappingElement child = element.addChildElement(new MappingElement("childNode"));//$NON-NLS-1$
        child.setSource("childNodeSource"); //$NON-NLS-1$
        
        String savedXML = saveMappingDocument(doc);
        
        assertEquals(expected, savedXML);        
    }
    
    public void testCriteria() throws Exception{
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" +  //$NON-NLS-1$                                                            
                "<mappingNode>" +  //$NON-NLS-1$
                    "<name>parentNode</name>" +  //$NON-NLS-1$
                    "<mappingNode>" +  //$NON-NLS-1$
                        "<nodeType>choice</nodeType>" +  //$NON-NLS-1$
                        "<exceptionOnDefault>true</exceptionOnDefault>"+ //$NON-NLS-1$
                        "<mappingNode>" +  //$NON-NLS-1$
                            "<nodeType>criteria</nodeType>" +  //$NON-NLS-1$            
                            "<criteria>childNodeCriteria</criteria>" +  //$NON-NLS-1$
                            "<mappingNode>" +  //$NON-NLS-1$
                                "<name>childNode</name>" +  //$NON-NLS-1$            
                            "</mappingNode>" +  //$NON-NLS-1$            
                        "</mappingNode>" +  //$NON-NLS-1$            
                    "</mappingNode>" +  //$NON-NLS-1$            
                "</mappingNode>" +  //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        
        
        MappingDocument doc = new MappingDocument(false);
        MappingElement element = doc.addChildElement(new MappingElement("parentNode")); //$NON-NLS-1$
        MappingChoiceNode choice = element.addChoiceNode(new MappingChoiceNode(true));
        MappingCriteriaNode criteria = choice.addCriteriaNode(new MappingCriteriaNode("childNodeCriteria", false)); //$NON-NLS-1$
        criteria.addChildElement(new MappingElement("childNode")); //$NON-NLS-1$

        String savedXML = saveMappingDocument(doc);
        
        assertEquals(expected, savedXML);        
    }
    
    public void testElement() throws Exception{
        String expected  = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" +  //$NON-NLS-1$                                                                        
                "<mappingNode>" +  //$NON-NLS-1$
                    "<name>parentNode</name>" +  //$NON-NLS-1$
                    "<default>ddd</default>" +  //$NON-NLS-1$
                    "<fixed>fff</fixed>" +  //$NON-NLS-1$
                    "<optional>true</optional>" +             //$NON-NLS-1$
                    "<isNillable>true</isNillable>" +  //$NON-NLS-1$
                    "<isExcluded>true</isExcluded>" +  //$NON-NLS-1$            
                    "<textNormalization>replace</textNormalization>" +  //$NON-NLS-1$
                    "<builtInType>decimal</builtInType>" + //$NON-NLS-1$
               "</mappingNode>" +  //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        
        MappingDocument doc = new MappingDocument(false);
        MappingElement element = doc.addChildElement(new MappingElement("parentNode")); //$NON-NLS-1$
        element.setNillable(true);
        element.setExclude(true);
        element.setDefaultValue("ddd"); //$NON-NLS-1$
        element.setValue("fff"); //$NON-NLS-1$
        element.setOptional(true);
        element.setNormalizeText("replace"); //$NON-NLS-1$
        element.setType("decimal"); //$NON-NLS-1$
        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);        
    }
    
    public void testRecursiveNode() throws Exception{
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
            "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
            "<formattedDocument>false</formattedDocument>" +  //$NON-NLS-1$                                                                        
            "<mappingNode>" +  //$NON-NLS-1$
                "<name>parentNode</name>" +  //$NON-NLS-1$
                "<source>parentNodeSource</source>" +  //$NON-NLS-1$
                "<mappingNode>" +  //$NON-NLS-1$
                    "<name>childNode</name>" +  //$NON-NLS-1$                    
                    "<mappingNode>" +  //$NON-NLS-1$            
                        "<name>attributename</name>" +  //$NON-NLS-1$
                        "<nodeType>attribute</nodeType>" +  //$NON-NLS-1$                            
                        "<default>ddd</default>" +  //$NON-NLS-1$
                        "<fixed>fff</fixed>" +  //$NON-NLS-1$            
                    "</mappingNode>" +  //$NON-NLS-1$
                    "<mappingNode>" +  //$NON-NLS-1$            
                        "<name>recursivenodename</name>" +  //$NON-NLS-1$
                        "<isRecursive>true</isRecursive>" +  //$NON-NLS-1$
                        "<recursionCriteria>rrr</recursionCriteria>" +  //$NON-NLS-1$
                        "<recursionLimit>8</recursionLimit>" +  //$NON-NLS-1$
                        "<recursionRootMappingClass>parentNodeSource</recursionRootMappingClass>" +  //$NON-NLS-1$            
                    "</mappingNode>" +  //$NON-NLS-1$
                "</mappingNode>" +  //$NON-NLS-1$            
            "</mappingNode>" +  //$NON-NLS-1$                
            "</xmlMapping>"; //$NON-NLS-1$
        
        
        MappingDocument doc = new MappingDocument(false);
        MappingElement parentNode = doc.addChildElement(new MappingElement("parentNode")); //$NON-NLS-1$
        parentNode.setSource("parentNodeSource"); //$NON-NLS-1$
        MappingElement childNode = parentNode.addChildElement(new MappingElement("childNode"));//$NON-NLS-1$
        MappingAttribute attribute = new MappingAttribute("attributename"); //$NON-NLS-1$
        childNode.addAttribute(attribute);
        attribute.setDefaultValue("ddd"); //$NON-NLS-1$
        attribute.setValue("fff"); //$NON-NLS-1$
        MappingRecursiveElement recursiveElement = (MappingRecursiveElement)childNode.addChildElement(new MappingRecursiveElement("recursivenodename", "parentNodeSource"));//$NON-NLS-1$ //$NON-NLS-2$
        recursiveElement.setRecursionLimit(8, false);
        recursiveElement.setCriteria("rrr"); //$NON-NLS-1$
        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);         
    }
        
    public void testMoveNamespaceDeclaration() throws Exception {
        String actual = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "        <mappingNode>\r\n" +  //$NON-NLS-1$
            "            <name>xsi</name>\r\n" +  //$NON-NLS-1$
            "            <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$
            "            <namespace>xmlns</namespace>\r\n" +  //$NON-NLS-1$
            "            <fixed>http://some.uri/</fixed>\r\n" +  //$NON-NLS-1$
            "        </mappingNode>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        

        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$
                "<mappingNode>" + //$NON-NLS-1$
                    "<namespaceDeclaration><prefix>xsi</prefix><uri>http://some.uri/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<name>parentNode</name>" + //$NON-NLS-1$
                    "<includeAlways>false</includeAlways>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
                "</xmlMapping>";  //$NON-NLS-1$
        
        MappingDocument doc = loadMappingDocument(actual);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);         
    }
    
    public void testMoveNamespaceDeclaration2() throws Exception {
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$
                "<mappingNode>" + //$NON-NLS-1$
                    "<namespaceDeclaration><prefix>xsi</prefix><uri>http://some.uri/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<name>parentNode</name>" + //$NON-NLS-1$
                    "<includeAlways>false</includeAlways>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
                "</xmlMapping>";  //$NON-NLS-1$
        
        MappingDocument doc = loadMappingDocument(expected);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);         
    }    
    
    public void testMoveDefaultNamespaceDeclaration() throws Exception {
        String actual = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>license</name>\r\n" +  //$NON-NLS-1$
            "        <mappingNode>\r\n" +  //$NON-NLS-1$
            "            <name>xmlns</name>\r\n" +  //$NON-NLS-1$
            "            <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$
            "            <fixed>http://some.uri/</fixed>\r\n" +  //$NON-NLS-1$
            "        </mappingNode>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$

        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$
                "<mappingNode>" + //$NON-NLS-1$
                    "<namespaceDeclaration><uri>http://some.uri/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<name>license</name>" + //$NON-NLS-1$
                    "<includeAlways>false</includeAlways>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        MappingDocument doc = loadMappingDocument(actual);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);                 
    }

    public void testMoveDefaultNamespaceDeclaration2() throws Exception {
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$
                "<mappingNode>" + //$NON-NLS-1$
                    "<namespaceDeclaration><uri>http://some.uri/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<name>license</name>" + //$NON-NLS-1$
                    "<includeAlways>false</includeAlways>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        MappingDocument doc = loadMappingDocument(expected);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);                 
    }
    
    public void testMoveNamespaceDeclarations() throws Exception {
        String actual = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>license</name>\r\n" +  //$NON-NLS-1$
            "        <mappingNode>\r\n" +  //$NON-NLS-1$
            "            <name>xsi</name>\r\n" +  //$NON-NLS-1$
            "            <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$
            "            <namespace>xmlns</namespace>\r\n" +  //$NON-NLS-1$
            "            <fixed>http://some.uri/</fixed>\r\n" +  //$NON-NLS-1$
            "        </mappingNode>\r\n" +  //$NON-NLS-1$
            "        <mappingNode>\r\n" +  //$NON-NLS-1$
            "            <name>xmlns</name>\r\n" +  //$NON-NLS-1$
            "            <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$
            "            <fixed>http://some.uri2/</fixed>\r\n" +  //$NON-NLS-1$
            "        </mappingNode>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$
                "<mappingNode>" + //$NON-NLS-1$
                    "<namespaceDeclaration><prefix>xsi</prefix><uri>http://some.uri/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<namespaceDeclaration><uri>http://some.uri2/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<name>license</name>" + //$NON-NLS-1$
                    "<includeAlways>false</includeAlways>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        MappingDocument doc = loadMappingDocument(actual);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);        
    }
    
    public void testMoveNamespaceDeclarations2() throws Exception {      
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$
                "<mappingNode>" + //$NON-NLS-1$
                    "<namespaceDeclaration><prefix>xsi</prefix><uri>http://some.uri/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<namespaceDeclaration><uri>http://some.uri2/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<name>license</name>" + //$NON-NLS-1$
                    "<includeAlways>false</includeAlways>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        MappingDocument doc = loadMappingDocument(expected);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);        
    }    
    
    // Sometimes a namespace may be used before its declaration; 
    public void testUseNamespaceBeforeDeclaration() throws Exception {
        String actual = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>license</name>\r\n" +  //$NON-NLS-1$
            "        <mappingNode>\r\n" +  //$NON-NLS-1$
            "            <name>usenamespace</name>\r\n" +  //$NON-NLS-1$
            "            <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$
            "            <namespace>foo</namespace>\r\n" +  //$NON-NLS-1$                  
            "        </mappingNode>\r\n" +  //$NON-NLS-1$            
            "        <mappingNode>\r\n" +  //$NON-NLS-1$
            "            <name>foo</name>\r\n" +  //$NON-NLS-1$
            "            <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$
            "            <namespace>xmlns</namespace>\r\n" +  //$NON-NLS-1$
            "            <fixed>http://some.uri/</fixed>\r\n" +  //$NON-NLS-1$
            "        </mappingNode>\r\n" +  //$NON-NLS-1$
            "        <mappingNode>\r\n" +  //$NON-NLS-1$
            "            <name>xmlns</name>\r\n" +  //$NON-NLS-1$
            "            <nodeType>attribute</nodeType>\r\n" +  //$NON-NLS-1$
            "            <fixed>http://some.uri2/</fixed>\r\n" +  //$NON-NLS-1$
            "        </mappingNode>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$
                "<mappingNode>" + //$NON-NLS-1$
                    "<namespaceDeclaration><prefix>foo</prefix><uri>http://some.uri/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<namespaceDeclaration><uri>http://some.uri2/</uri></namespaceDeclaration>" + //$NON-NLS-1$
                    "<name>license</name>" + //$NON-NLS-1$
                    "<includeAlways>false</includeAlways>" + //$NON-NLS-1$
                    "<mappingNode>" + //$NON-NLS-1$
                        "<name>usenamespace</name>" + //$NON-NLS-1$
                        "<nodeType>attribute</nodeType>" + //$NON-NLS-1$
                        "<namespace>foo</namespace>" + //$NON-NLS-1$
                        "<includeAlways>false</includeAlways>"+ //$NON-NLS-1$                        
                    "</mappingNode>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
            "</xmlMapping>"; //$NON-NLS-1$
        MappingDocument doc = loadMappingDocument(actual);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);        
    }    
    
    public void testRecursive() throws Exception {
        String expected = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" + //$NON-NLS-1$
                "<formattedDocument>false</formattedDocument>" + //$NON-NLS-1$            
                "<mappingNode>" +  //$NON-NLS-1$                
                    "<name>parentNode</name>" +  //$NON-NLS-1$
                    "<source>parentSource</source>" +  //$NON-NLS-1$                    
                    "<includeAlways>false</includeAlways>"+ //$NON-NLS-1$
                    "<mappingNode>"+ //$NON-NLS-1$
                        "<name>childNode</name>" +  //$NON-NLS-1$
                        "<source>childSource</source>" +  //$NON-NLS-1$
                        "<isRecursive>true</isRecursive>"+ //$NON-NLS-1$
                        "<recursionCriteria>foo</recursionCriteria>" + //$NON-NLS-1$
                        "<recursionLimit>6</recursionLimit>" + //$NON-NLS-1$                                
                        "<recursionRootMappingClass>parentSource</recursionRootMappingClass>" + //$NON-NLS-1$
                        "<includeAlways>false</includeAlways>"+ //$NON-NLS-1$
                    "</mappingNode>"+ //$NON-NLS-1$
                "</mappingNode>"+ //$NON-NLS-1$                
            "</xmlMapping>"; //$NON-NLS-1$        
        
        MappingDocument doc = loadMappingDocument(expected);        
        String savedXML = saveMappingDocument(doc);
        assertEquals(expected, savedXML);        
        
    }
} 
