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

import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.FileStore;
import org.teiid.core.types.Streamable;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.processor.xml.NodeDescriptor;
import org.teiid.query.processor.xml.DocumentInProgress;

import junit.framework.TestCase;


public class TestSAXDocumentInProgress extends TestCase {
    
    public static String originalText = "  Hello\t\t my \n    \n tests for preserve, \t \r\n replace, collapse.\n  "; //$NON-NLS-1$
	public TestSAXDocumentInProgress(String name) {
		super(name);
	}
    
    public void testLargeDocument()throws Exception{
		FileStore fs = BufferManagerFactory.getStandaloneBufferManager().createFileStore("test"); //$NON-NLS-1$
    	DocumentInProgress doc = new DocumentInProgress(fs, Streamable.ENCODING);
    	//long startTime = System.currentTimeMillis();
    	doc.setDocumentFormat(true);
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("Root", null, true, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        doc.addElement(descriptor, (NodeDescriptor)null);    	
        doc.moveToLastChild();
        descriptor = NodeDescriptor.createNodeDescriptor("a1", null, false, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$        
    	doc.addAttribute(descriptor, "test1");//$NON-NLS-1$ 
        descriptor = NodeDescriptor.createNodeDescriptor("a1", null, false, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
    	doc.addAttribute(descriptor, "test2");//$NON-NLS-1$ 
        descriptor = NodeDescriptor.createNodeDescriptor("Child", null, true, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$ 
    	for (int i = 0; i < 50; i++) {
    		doc.addElement(descriptor, "test content");//$NON-NLS-1$ 
    	}
    	doc.moveToParent();
    	doc.markAsFinished();
    	//char[] chunk = doc.getNextChunk(10);
    	//System.out.println("Got chunk " + j + " length="+chunk.length);//$NON-NLS-1$ //$NON-NLS-2$
    	//System.out.println("Total processing time = "  + (System.currentTimeMillis() - startTime) + " Total elements added " + i); //$NON-NLS-1$ //$NON-NLS-2$
    	//System.out.println(chunk);
    }
    
    public void testNormalizationPreserve() throws Exception{
        assertEquals(DocumentInProgress.normalizeText(originalText,MappingNodeConstants.NORMALIZE_TEXT_PRESERVE), originalText);
    }
    public void testNormalizationReplace() throws Exception{
        String expectedResult = "  Hello   my        tests for preserve,      replace, collapse.   "; //$NON-NLS-1$
        assertEquals(DocumentInProgress.normalizeText(originalText,MappingNodeConstants.NORMALIZE_TEXT_REPLACE), expectedResult);
    }
    public void testNormalizationCollapse() throws Exception{
        String expectedResult = "Hello my tests for preserve, replace, collapse."; //$NON-NLS-1$
        assertEquals(DocumentInProgress.normalizeText(originalText,MappingNodeConstants.NORMALIZE_TEXT_COLLAPSE), expectedResult);
    }
}
