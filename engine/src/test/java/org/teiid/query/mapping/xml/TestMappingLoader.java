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
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingBaseNode;
import org.teiid.query.mapping.xml.MappingChoiceNode;
import org.teiid.query.mapping.xml.MappingCriteriaNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingException;
import org.teiid.query.mapping.xml.MappingLoader;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.Namespace;

import junit.framework.TestCase;


/**
 * <p>Test cases for {@link MappingLoader} class. </p>
 */
public class TestMappingLoader extends TestCase {

    // Validation should succeed
    private static final boolean VALID = true;

    // Validation should not succeed
    private static final boolean INVALID = false;

    // Name of example mapping file
    private static final String MAPPING_FILE = "LicenseMappingExample.xml"; //$NON-NLS-1$

    // Name of example mapping file
    private static final String PARTS_MAPPING_FILE = "PartsMappingExample.xml"; //$NON-NLS-1$

    // =========================================================================
    //                        T E S T     C O N T R O L
    // =========================================================================

    /** Construct test case. */
    public TestMappingLoader( String name ) {
        super( name );
    }


    // =========================================================================
    //                       W O R K     M E T H O D S
    // =========================================================================

    /** Gets the absolute path to a file in the test data path. */
    static String getFilePathInDataDir( String filename ) {
        // Get a File for the source file, and make sure it exists
        File file = new File( UnitTestUtil.getTestDataPath(), filename );
        //assertTrue( "File '" + filename + "' with absolute path '" 
        //    + file.getAbsolutePath() + "' does not exist in test data directory.",
        //    file.exists() );
        return file.getAbsolutePath();
    }

    /** Load a mapping definition from a file. */
    private static MappingNode loadFromFile( String filename ) 
        throws Exception {
        String fileAbsolutePath = getFilePathInDataDir(filename);
        MappingLoader loader = new MappingLoader();
        return loader.loadDocument(fileAbsolutePath);
    }

    /** Load a mapping definition from a stream. */
    private static MappingNode loadFromStream( String filename ) 
        throws Exception {
        InputStream istream = getResourceStream( MappingLoader.class, filename );
        if ( istream == null ) {
            throw new IllegalStateException( "File " + filename //$NON-NLS-1$
                + " is not in the application's classpath." ); //$NON-NLS-1$
        }
        MappingLoader loader= new MappingLoader();
        return loader.loadDocument(istream);
    }


    /** 
     * <p>Utility to get an input stream to a file in the app's classpath. </p>
     */
    private static InputStream getResourceStream( Class appClass, String filename ) {
        return appClass.getResourceAsStream(filename);
    }

    // =========================================================================
    //                      H E L P E R    M E T H O D S
    // =========================================================================

    /**
     * Test license in specified file for specified product, version, and IP.
     * Whether or not the check should pass is specified.
     */
    private MappingNode helpLoad( String filename, boolean fromStream, 
                            boolean shouldSucceed ) {

        if ( fromStream ) {
            try {
                return loadFromStream( filename );
            } catch ( Exception e ) {
                if ( shouldSucceed ) {
                    fail( "File " + filename  //$NON-NLS-1$
                      + " could not be loaded from the application's classpath:" //$NON-NLS-1$
                      + e.getMessage() );
                } else {
                    // ok
                }
            }
        } else {
            try {
                return loadFromFile( filename );
            } catch ( Exception e ) {
                if ( shouldSucceed ) {
                    fail( "File " + filename  //$NON-NLS-1$
                      + " could not be loaded from the file system: " //$NON-NLS-1$
                      + e.getMessage() );
                } else {
                    // ok
                }
            }
        }
        return null;
    }

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    /**
     * Positive test -- load from file in app classpath.
     */
    public void testPos_LoadFromStream() {
        helpLoad( MAPPING_FILE, true, VALID ); 
    }

    /**
     * Negative test -- attempt load from file that's not in file system.
     */
    public void testNeg_LoadFromFile() {
        helpLoad( "InValidFile.yada", false, INVALID ); //$NON-NLS-1$
    }

    /**
     * Negative test -- attempt load from file that's not in app classpath.
     */
    public void testNeg_LoadFromStream() {
        helpLoad( "InValidFile.yada", true, INVALID ); //$NON-NLS-1$
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
        
        MappingDocument doc = loadMappingDocument(xml);

        
        // check source node
        MappingBaseNode root = doc.getRootNode();
        assertTrue(root instanceof MappingElement);
        MappingElement element = (MappingElement)root;
        
        // check the staging tables
        List list = element.getStagingTables();
        assertEquals(2, list.size());

        assertEquals("testTempGroup1", list.get(0)); //$NON-NLS-1$
        assertEquals("testTempGroup2", list.get(1)); //$NON-NLS-1$
        
        // make sure name matches and caridinality of root is reset; as there can be only one root
        assertEquals("license", element.getName()); //$NON-NLS-1$
        assertEquals(1, element.getMinOccurence());
        assertEquals(1, element.getMaxOccurence());
        assertEquals("licenseSource", element.getSource()); //$NON-NLS-1$
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
        
        MappingDocument doc = loadMappingDocument(xml);    
                
        // check source node
        MappingBaseNode node = doc.getRootNode();
        assertTrue(node instanceof MappingElement);
        MappingElement element = (MappingElement)node;
        
        List list = element.getStagingTables();
        assertEquals(1, list.size());
        assertEquals("testTempGroup1", list.get(0)); //$NON-NLS-1$
        
        
        // make sure name matches and caridinality of root is reset; as there can be only one root
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        assertEquals(1, element.getMinOccurence());
        assertEquals(1, element.getMaxOccurence());
        
        MappingNode node1 = (MappingNode)element.getNodeChildren().get(0);
             
        // make sure source's child is mapping element and mapping element's source
        // is above source        
        assertTrue(node1 instanceof MappingElement);
        element = (MappingElement)node1;
        
        list = element.getStagingTables();
        assertEquals(1, list.size());
        assertEquals("testTempGroup2", list.get(0)); //$NON-NLS-1$
        
        assertEquals("childNode", element.getName()); //$NON-NLS-1$
        assertEquals("childNodeSource", element.getSource()); //$NON-NLS-1$
    }
       
    public void testCriteriaXML50() throws Exception{
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <mappingNode>\r\n" +  //$NON-NLS-1$
            "           <nodeType>choice</nodeType>\r\n" +  //$NON-NLS-1$
            "           <exceptionOnDefault>true</exceptionOnDefault>"+ //$NON-NLS-1$
            "           <mappingNode>\r\n" +  //$NON-NLS-1$
            "               <name>childNode</name>\r\n" +  //$NON-NLS-1$
            "               <criteria>childNodeCriteria</criteria>\r\n" +  //$NON-NLS-1$            
            "           </mappingNode>\r\n" +  //$NON-NLS-1$            
            "       </mappingNode>\r\n" +  //$NON-NLS-1$            
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        MappingDocument doc = loadMappingDocument(xml);   
        
        MappingNode node = doc.getRootNode();
        assertTrue(node instanceof MappingElement);
        
        MappingElement element = (MappingElement)node;
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        
        node = (MappingNode)element.getNodeChildren().get(0);
        assertTrue(node instanceof MappingChoiceNode);
        MappingChoiceNode choice = (MappingChoiceNode)node;
        assertTrue(choice.throwExceptionOnDefault());
        
        node = (MappingNode)choice.getNodeChildren().get(0);
        assertTrue(node instanceof MappingCriteriaNode);
        MappingCriteriaNode criteria = (MappingCriteriaNode)node;
        assertEquals("childNodeCriteria", criteria.getCriteria()); //$NON-NLS-1$
        
        node = (MappingNode)criteria.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);
        element = (MappingElement)node;
        assertEquals("childNode", element.getName()); //$NON-NLS-1$
        
    }
    
    public void testCriteriaXML55() throws Exception{
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <mappingNode>\r\n" +  //$NON-NLS-1$
            "           <nodeType>choice</nodeType>\r\n" +  //$NON-NLS-1$
            "           <exceptionOnDefault>true</exceptionOnDefault>"+ //$NON-NLS-1$
            "           <mappingNode>\r\n" +  //$NON-NLS-1$
            "               <nodeType>criteria</nodeType>\r\n" +  //$NON-NLS-1$            
            "               <criteria>childNodeCriteria</criteria>\r\n" +  //$NON-NLS-1$
            "               <mappingNode>\r\n" +  //$NON-NLS-1$
            "                   <name>childNode</name>\r\n" +  //$NON-NLS-1$            
            "               </mappingNode>\r\n" +  //$NON-NLS-1$            
            "           </mappingNode>\r\n" +  //$NON-NLS-1$            
            "       </mappingNode>\r\n" +  //$NON-NLS-1$            
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        
        MappingDocument doc = loadMappingDocument(xml);   
        
        MappingNode node = doc.getRootNode();
        assertTrue(node instanceof MappingElement);
        
        MappingElement element = (MappingElement)node;
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        
        node = (MappingNode)element.getNodeChildren().get(0);
        assertTrue(node instanceof MappingChoiceNode);
        MappingChoiceNode choice = (MappingChoiceNode)node;
        assertTrue(choice.throwExceptionOnDefault());
        
        node = (MappingNode)choice.getNodeChildren().get(0);
        assertTrue(node instanceof MappingCriteriaNode);
        MappingCriteriaNode criteria = (MappingCriteriaNode)node;
        assertEquals("childNodeCriteria", criteria.getCriteria()); //$NON-NLS-1$
        
        node = (MappingNode)criteria.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);
        element = (MappingElement)node;
        assertEquals("childNode", element.getName()); //$NON-NLS-1$
    }
    
    public void testElement() throws Exception{
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "        <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "        <isNillable>TRUE</isNillable>\r\n" +  //$NON-NLS-1$
            "        <isExcluded>TRUE</isExcluded>\r\n" +  //$NON-NLS-1$            
            "        <default>ddd</default>\r\n" +  //$NON-NLS-1$
            "        <fixed>fff</fixed>\r\n" +  //$NON-NLS-1$
            "        <optional>TRUE</optional>\r\n" +             //$NON-NLS-1$
            "        <textNormalization>replace</textNormalization>\r\n" +  //$NON-NLS-1$
            "        <builtInType>decimal</builtInType>\r\n" + //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
     
        MappingDocument doc = loadMappingDocument(xml);   
        
        MappingNode node = doc.getRootNode();
        assertTrue(node instanceof MappingElement);        
        MappingElement element = (MappingElement)node;
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        assertFalse(element.isRootRecursiveNode());
        assertFalse(element.isRecursive());
        assertTrue(element.isNillable());
        assertTrue(element.isExcluded());
        assertTrue(element.isOptional());
        assertEquals("ddd", element.getDefaultValue()); //$NON-NLS-1$
        assertEquals("fff", element.getValue()); //$NON-NLS-1$
        assertEquals("replace", element.getNormalizeText()); //$NON-NLS-1$
        assertEquals("decimal", element.getType()); //$NON-NLS-1$        
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
        
        MappingDocument doc = loadMappingDocument(xml);   
        
        MappingNode node = doc.getRootNode();
        assertTrue(node instanceof MappingElement);        
        MappingElement element = (MappingElement)node;
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        assertTrue(element.isRootRecursiveNode());
        assertFalse(element.isRecursive());
        assertEquals("parentNodeSource", element.getSource()); //$NON-NLS-1$
        
        node = (MappingNode)element.getNodeChildren().get(0);
        assertTrue(node instanceof MappingElement);        
        element = (MappingElement)node;
        assertEquals("childNode", element.getName()); //$NON-NLS-1$

        List attrs = element.getAttributes();
        assertEquals(1, attrs.size());
        MappingAttribute attribute = (MappingAttribute)attrs.get(0);
        assertEquals("attributename", attribute.getName()); //$NON-NLS-1$
        assertEquals("ddd", attribute.getDefaultValue()); //$NON-NLS-1$
        assertEquals("fff", attribute.getValue()); //$NON-NLS-1$
        
        node = (MappingNode)element.getNodeChildren().get(0);
        assertTrue(node instanceof MappingRecursiveElement);        
        MappingRecursiveElement recursive = (MappingRecursiveElement)node;
        assertEquals("recursivenodename", recursive.getName()); //$NON-NLS-1$
        assertEquals(8, recursive.getRecursionLimit());
        assertFalse(recursive.isRootRecursiveNode());
        assertTrue(recursive.isRecursive());
    }

    public void testParseNamespaces() {
        /*MappingNode m = */helpLoad( PARTS_MAPPING_FILE, false, VALID );
        //MappingNode.printMappingNodeTree(m, System.out);
    }

    
    private MappingDocument loadMappingDocument(String xml) throws MappingException {
            MappingLoader reader = new MappingLoader();
    
            byte[] bytes = xml.getBytes();
    
            InputStream istream = new ByteArrayInputStream (bytes);
            
            return reader.loadDocument(istream);
    }
            
    /** 
     * test of the adjustment XMLPlanner must do to the way 
     * namespace declarations are constructed by the modeler
     * in the mapping doc and the way the XMLPlanner must see
     * them. 
     */
    public void testMoveNamespaceDeclaration() throws Exception {
        String xml = 
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
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
        
        MappingDocument doc = loadMappingDocument(xml);
        
        Properties namespaceDeclarations = ((MappingElement)doc.getRootNode()).getNamespacesAsProperties();
        
        assertNotNull(namespaceDeclarations);
        
        String uri = namespaceDeclarations.getProperty("xsi"); //$NON-NLS-1$
        assertEquals("http://some.uri/", uri); //$NON-NLS-1$
    }
    
    /** 
     * test of the adjustment XMLPlanner must do to the way 
     * namespace declarations are constructed by the modeler
     * in the mapping doc and the way the XMLPlanner must see
     * them. 
     */
    public void testMoveDefaultNamespaceDeclaration() throws Exception {
        String xml = 
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
  
            MappingDocument doc = loadMappingDocument(xml);
            
            Properties namespaceDeclarations = ((MappingElement)doc.getRootNode()).getNamespacesAsProperties();            
            assertNotNull(namespaceDeclarations);
            
            String uri = namespaceDeclarations.getProperty(MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX); 
            assertEquals("http://some.uri/", uri); //$NON-NLS-1$
    }
    
    /** 
     * test of the adjustment XMLPlanner must do to the way 
     * namespace declarations are constructed by the modeler
     * in the mapping doc and the way the XMLPlanner must see
     * them. 
     */
    public void testMoveNamespaceDeclarations() throws Exception {
        
        String xml = 
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
        
        MappingDocument doc = loadMappingDocument(xml);
        Properties namespaceDeclarations = ((MappingElement)doc.getRootNode()).getNamespacesAsProperties();            
        
        assertNotNull(namespaceDeclarations);
        
        String uri = namespaceDeclarations.getProperty("xsi"); //$NON-NLS-1$
        assertEquals("http://some.uri/", uri); //$NON-NLS-1$
        
        String uri2 = namespaceDeclarations.getProperty(MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX); 
        assertEquals("http://some.uri2/", uri2); //$NON-NLS-1$        
    }      
        
    // Sometimes a namespace may be used before its declaration; 
    public void testUseNamespaceBeforeDeclaration() throws Exception {
        String xml = 
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
        
        MappingDocument doc = loadMappingDocument(xml);
        
        assertEquals("license", doc.getRootNode().getName()); //$NON-NLS-1$
        
        MappingElement license = (MappingElement)doc.getRootNode();
        MappingAttribute usenamespace = (MappingAttribute)license.getAttributes().get(0);
        
        assertEquals("usenamespace", usenamespace.getName()); //$NON-NLS-1$
        
        // by the time the document is loaded the namspace must have been resolved.
        Namespace ns = usenamespace.getNamespace();            
        assertEquals("foo", ns.getPrefix()); //$NON-NLS-1$
        assertEquals("http://some.uri/", ns.getUri()); //$NON-NLS-1$
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
     
        MappingDocument doc = loadMappingDocument(xml);   
        
        MappingNode node = doc.getRootNode();
        
        // parent element
        assertTrue(node instanceof MappingElement);        
        MappingElement element = (MappingElement)node;
        assertEquals("parentNode", element.getName()); //$NON-NLS-1$
        assertTrue(element.isRootRecursiveNode());
        assertFalse(element.isRecursive());
        assertEquals("parentSource", element.getSource()); //$NON-NLS-1$
        
        // recursive source
        node = (MappingNode)element.getNodeChildren().get(0);
        assertTrue(node instanceof MappingRecursiveElement);        
        MappingRecursiveElement relement = (MappingRecursiveElement)node;
        
        assertTrue(relement.isRecursive());
        assertEquals("childNode", relement.getName()); //$NON-NLS-1$
        assertEquals("foo", relement.getCriteria()); //$NON-NLS-1$
        assertEquals(6, relement.getRecursionLimit());
        assertEquals("childSource", relement.getSource()); //$NON-NLS-1$
        assertEquals("parentSource", relement.getMappingClass()); //$NON-NLS-1$
    }   
    
    public void testEncoding() throws Exception {
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <documentEncoding>windows-1252</documentEncoding>>\r\n" +  //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "       <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <source>parentSource</source>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
  
        MappingDocument doc = loadMappingDocument(xml);
        assertEquals("windows-1252", doc.getDocumentEncoding()); //$NON-NLS-1$
    }
    
    public void testEncoding1() throws Exception {
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "   <mappingNode>\r\n" +  //$NON-NLS-1$
            "       <documentEncoding>foo</documentEncoding>>\r\n" +  //$NON-NLS-1$
            "       <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <source>parentSource</source>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
  
        MappingDocument doc = loadMappingDocument(xml);
        assertEquals("foo", doc.getDocumentEncoding()); //$NON-NLS-1$
    }
    
    public void testDocumentFormatted() throws Exception {
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <formattedDocument>true</formattedDocument>>\r\n" +  //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "       <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <source>parentSource</source>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
  
        MappingDocument doc = loadMappingDocument(xml);
        assertTrue(doc.isFormatted());
    }
    
    public void testDocumentFormatted1() throws Exception {
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <formattedDocument>false</formattedDocument>>\r\n" +  //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "       <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <source>parentSource</source>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
  
        MappingDocument doc = loadMappingDocument(xml);
        assertFalse(doc.isFormatted());
    }
    
    public void testDocumentFormatted2() throws Exception {
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "       <formattedDocument>true</formattedDocument>>\r\n" +  //$NON-NLS-1$
            "       <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "       <source>parentSource</source>\r\n" +  //$NON-NLS-1$
            "    </mappingNode>\r\n" +  //$NON-NLS-1$
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
  
        MappingDocument doc = loadMappingDocument(xml);
        assertTrue(doc.isFormatted());
    }
    
    public void testLoadNodeWithoutNameOrType() throws Exception {
        String xml = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<xmlMapping>\r\n" + //$NON-NLS-1$
            "    <mappingNode>\r\n" +  //$NON-NLS-1$
            "       <mappingNode>\r\n" +  //$NON-NLS-1$            
            "           <formattedDocument>true</formattedDocument>>\r\n" +  //$NON-NLS-1$
            "           <name>parentNode</name>\r\n" +  //$NON-NLS-1$
            "           <source>parentSource</source>\r\n" +  //$NON-NLS-1$
            "       </mappingNode>\r\n" +  //$NON-NLS-1$
            "   </mappingNode>\r\n" +  //$NON-NLS-1$            
            "</xmlMapping>\r\n\r\n"; //$NON-NLS-1$
  
        try {
            loadMappingDocument(xml);
            fail("must have failed to node, since the parent node does not have either name or type"); //$NON-NLS-1$
        } catch (MappingException e) {
            assertEquals("Null or blank name found in the Mapping Document, Must have valid name. Re-build the VDB", e.getMessage()); //$NON-NLS-1$
        }
    }    
                 
} // END CLASS
