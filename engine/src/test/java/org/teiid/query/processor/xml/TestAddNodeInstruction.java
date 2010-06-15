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

import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.FileStore;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.util.VariableContext;


/**
 * Unit tests {@link AddNodeInstruction} class
 */
public class TestAddNodeInstruction extends TestCase {
	public TestAddNodeInstruction(String name) {
		super(name);
	}

    private static final String RESULT_SET_NAME = "rsName".toUpperCase(); //$NON-NLS-1$
    private static final String STRING_COLUMN = ".stringValue"; //$NON-NLS-1$
    private static final String NULL_COLUMN = ".nullValue"; //$NON-NLS-1$

    public void testAddEmptyElement() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = true;
        String fixedValue = null;
        Properties namespaceDeclarations = null;
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, fixedValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor);
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root><test/></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testDontAddEmptyElement() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = true;
        String fixedValue = null;
        Properties namespaceDeclarations = null;
        boolean isOptional = true;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, fixedValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor);
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root/>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddElementWithContent() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = true;
        String rsColumn = STRING_COLUMN;
        String defaultValue = null;
        Properties namespaceDeclarations = null;
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
         
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root><test>Lamp</test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddElementWithDefault() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = true;
        String rsColumn = NULL_COLUMN;
        String defaultValue = "default"; //$NON-NLS-1$
        Properties namespaceDeclarations = null;
        boolean isOptional = false;
        
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null,isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root><test>default</test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddElementWithFixed() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = true;
        String fixedValue = "fixed"; //$NON-NLS-1$
        Properties namespaceDeclarations = null;
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, fixedValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor);
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root><test>fixed</test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddContentElementWithNamespacePrefix() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = "yyz"; //$NON-NLS-1$
        boolean isElement = true;
        String rsColumn = STRING_COLUMN;
        String defaultValue = null;
        boolean isOptional = false;

        Properties rootNamespaceDeclarations = new Properties();
        rootNamespaceDeclarations.setProperty("yyz", "http://my.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, rootNamespaceDeclarations, null,isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        String actualDoc = helpGetDocument(addNodeInstruction, rootNamespaceDeclarations);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root xmlns:yyz=\"http://my.namespace/\"><yyz:test>Lamp</yyz:test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddContentElementWithNamespaces() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = true;
        String rsColumn = STRING_COLUMN;
        String defaultValue = null;
        Properties namespaceDeclarations = new Properties();
        namespaceDeclarations.setProperty("yyz", "http://my.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null,isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root><test xmlns:yyz=\"http://my.namespace/\">Lamp</test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddContentElementWithNamespaces2() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = "yyz"; //$NON-NLS-1$
        boolean isElement = true;
        String rsColumn = STRING_COLUMN;
        String defaultValue = null;
        Properties namespaceDeclarations = new Properties();
        namespaceDeclarations.setProperty("yyz", "http://my.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null,isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root><yyz:test xmlns:yyz=\"http://my.namespace/\">Lamp</yyz:test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddContentElementWithNamespaces3() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = "yyz"; //$NON-NLS-1$
        boolean isElement = true;
        String rsColumn = STRING_COLUMN;
        String defaultValue = "shouldn't see"; //$NON-NLS-1$
        Properties namespaceDeclarations = new Properties();
        namespaceDeclarations.setProperty("yyz", "http://my.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        Properties rootNamespaceDeclarations = new Properties();
        rootNamespaceDeclarations.setProperty("yyz", "http://my.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$
        
        String actualDoc = helpGetDocument(addNodeInstruction, rootNamespaceDeclarations);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root xmlns:yyz=\"http://my.namespace/\"><yyz:test>Lamp</yyz:test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddContentElementWithConflictingNamespaces() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = "yyz"; //$NON-NLS-1$
        boolean isElement = true;
        String rsColumn = STRING_COLUMN;
        String defaultValue = "shouldn't see"; //$NON-NLS-1$
        Properties namespaceDeclarations = new Properties();
        namespaceDeclarations.setProperty("yyz", "http://myother.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        Properties rootNamespaceDeclarations = new Properties();
        rootNamespaceDeclarations.setProperty("yyz", "http://my.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$
        
        String actualDoc = helpGetDocument(addNodeInstruction, rootNamespaceDeclarations);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root xmlns:yyz=\"http://my.namespace/\"><yyz:test xmlns:yyz=\"http://myother.namespace/\">Lamp</yyz:test></root>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }


    public void testAddAttributeWithContent() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = false;
        String rsColumn = STRING_COLUMN;
        String defaultValue = null;
        Properties namespaceDeclarations = null;
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root test=\"Lamp\"/>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testDontAddEmptyAttribute() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = false;
        String rsColumn = NULL_COLUMN;
        String defaultValue = null;
        Properties namespaceDeclarations = null;
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root/>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddAttributeWithDefaultValue() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = false;
        String rsColumn = NULL_COLUMN;
        String defaultValue = "default"; //$NON-NLS-1$
        Properties namespaceDeclarations = null;
        boolean isOptional = false;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root test=\"default\"/>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }
    
    public void testAddAttributeWithFixedValue() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = null;
        boolean isElement = false;
        String fixedValue = "fixed"; //$NON-NLS-1$
        Properties namespaceDeclarations = null;
        boolean isOptional = true;

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, fixedValue, namespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor);
        
        String actualDoc = helpGetDocument(addNodeInstruction);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root test=\"fixed\"/>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }

    public void testAddAttributeWithNamespacePrefix() throws Exception {
        
        String tag = "test"; //$NON-NLS-1$
        String namespacePrefix = "yyz"; //$NON-NLS-1$
        boolean isElement = false;
        String rsColumn = STRING_COLUMN;
        String defaultValue = "shouldn't see"; //$NON-NLS-1$
        boolean isOptional = false;

        Properties rootNamespaceDeclarations = new Properties();
        rootNamespaceDeclarations.setProperty("yyz", "http://my.namespace/"); //$NON-NLS-1$ //$NON-NLS-2$

        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(tag, namespacePrefix, isElement, defaultValue, rootNamespaceDeclarations, null, isOptional, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);
        AddNodeInstruction addNodeInstruction = new AddNodeInstruction(descriptor, new ElementSymbol(RESULT_SET_NAME + rsColumn));
                
        String actualDoc = helpGetDocument(addNodeInstruction, rootNamespaceDeclarations);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
            "<root xmlns:yyz=\"http://my.namespace/\" yyz:test=\"Lamp\"/>"; //$NON-NLS-1$
        
        assertEquals("XML doc mismatch: ", expectedDoc, actualDoc);         //$NON-NLS-1$
    }


    // ===============================================================
    // TEST HELPERS
    // ===============================================================

    /**
     * Take instruction, make a Program out of it, instantiate a
     * FakeEnvironment and XMLPlan, process instruction, and return result
     * doc
     * @throws TeiidComponentException
     */
    private String helpGetDocument(AddNodeInstruction addNodeInstruction) throws Exception{
        return helpGetDocument(addNodeInstruction, null);
    }

    /**
     * Take instruction, make a Program out of it, instantiate a
     * FakeEnvironment and XMLPlan, process instruction, and return result
     * doc
     * @throws TeiidComponentException
     */
    private String helpGetDocument(ProcessorInstruction addNodeInstruction, Properties namespaceDeclarations) throws Exception{
        
        XMLContext context = new XMLContext();
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();

        VariableContext varContext = context.getVariableContext();
        
        varContext.setValue(new ElementSymbol(RESULT_SET_NAME + STRING_COLUMN), "Lamp"); //$NON-NLS-1$
        varContext.setValue(new ElementSymbol(RESULT_SET_NAME + NULL_COLUMN), null);
        
        Program program = new Program();
        program.addInstruction(addNodeInstruction);
        
		env.pushProgram(program);
        
		FileStore fs = BufferManagerFactory.getStandaloneBufferManager().createFileStore("test"); //$NON-NLS-1$
        DocumentInProgress doc = new DocumentInProgress(fs, Streamable.ENCODING);
        env.setDocumentInProgress(doc);
        
        //add fake root, move to child
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("root", null, true, null, namespaceDeclarations, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE); //$NON-NLS-1$

        doc.addElement(descriptor, (NodeDescriptor)null); 
        doc.moveToLastChild();
                    
        addNodeInstruction.process(env, context);

        doc.moveToParent();
        doc.markAsFinished();
        
        String actualDoc = ObjectConverterUtil.convertToString(doc.getSQLXML().getCharacterStream());
        return actualDoc;    
    }

}
