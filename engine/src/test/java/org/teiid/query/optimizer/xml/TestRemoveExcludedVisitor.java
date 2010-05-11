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

package org.teiid.query.optimizer.xml;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingOutputter;
import org.teiid.query.optimizer.xml.XMLPlanner;

import junit.framework.TestCase;



/** 
 * Test RemoveExcludedVisitor
 */
public class TestRemoveExcludedVisitor extends TestCase {
    
    public void testRemoveExcluded() throws Exception {
        MappingDocument doc = FakeXMLMetadata.docWithExcluded();
        
        // remove excluded parts
        XMLPlanner.removeExcluded(doc);
        
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + //$NON-NLS-1$
                "<xmlMapping>" + //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>true</formattedDocument>" +  //$NON-NLS-1$                                                                
                "<mappingNode>" + //$NON-NLS-1$
                    "<name>root</name>" + //$NON-NLS-1$
                    "<mappingNode>" + //$NON-NLS-1$
                        "<name>element1</name>" + //$NON-NLS-1$
                        "<source>mappingclass1</source>" + //$NON-NLS-1$
                        "<symbol>nis_element1</symbol>" + //$NON-NLS-1$
                        "<mappingNode>" + //$NON-NLS-1$
                            "<name>element1</name>" + //$NON-NLS-1$
                            "<isRecursive>true</isRecursive>" + //$NON-NLS-1$
                            "<recursionRootMappingClass>mappingclass1</recursionRootMappingClass>" + //$NON-NLS-1$
                            "<mappingNode>"+ //$NON-NLS-1$
                                "<name>donotdelete</name>"+ //$NON-NLS-1$
                                "<symbol>nis_donotdelete</symbol>"+ //$NON-NLS-1$
                                "<isExcluded>true</isExcluded>"+ //$NON-NLS-1$
                            "</mappingNode>"+ //$NON-NLS-1$
                        "</mappingNode>" + //$NON-NLS-1$
                    "</mappingNode>" + //$NON-NLS-1$
                    "<mappingNode>" + //$NON-NLS-1$
                        "<nodeType>choice</nodeType>" + //$NON-NLS-1$
                        "<mappingNode>" + //$NON-NLS-1$
                            "<nodeType>criteria</nodeType>"+ //$NON-NLS-1$
                            "<criteria>one==one</criteria>" + //$NON-NLS-1$                        
                            "<mappingNode>" + //$NON-NLS-1$
                                "<name>c2_Criteria_1</name>" + //$NON-NLS-1$
                            "</mappingNode>" + //$NON-NLS-1$
                        "</mappingNode>" + //$NON-NLS-1$
                        "<mappingNode>" + //$NON-NLS-1$
                            "<nodeType>criteria</nodeType>" + //$NON-NLS-1$
                            "<isDefaultChoice>true</isDefaultChoice>" + //$NON-NLS-1$
                        "</mappingNode>" + //$NON-NLS-1$
                    "</mappingNode>" + //$NON-NLS-1$
                    "<mappingNode>" + //$NON-NLS-1$
                        "<nodeType>sequence</nodeType>" + //$NON-NLS-1$
                            "<mappingNode>" + //$NON-NLS-1$
                                "<name>seq1_element2</name>" + //$NON-NLS-1$
                                "<symbol>nis_seq1_element2</symbol>"+ //$NON-NLS-1$
                                "<isNillable>true</isNillable>"+ //$NON-NLS-1$
                            "</mappingNode>" + //$NON-NLS-1$
                    "</mappingNode>" + //$NON-NLS-1$
                    "<mappingNode>" + //$NON-NLS-1$
                        "<name>element2</name>" + //$NON-NLS-1$
                        "<mappingNode>" + //$NON-NLS-1$
                            "<nodeType>comment</nodeType>" + //$NON-NLS-1$
                            "<comment>this is comment</comment>" + //$NON-NLS-1$
                        "</mappingNode>" + //$NON-NLS-1$
                        "<mappingNode>" + //$NON-NLS-1$
                            "<name>element2_attribute2</name>" + //$NON-NLS-1$
                            "<nodeType>attribute</nodeType>" + //$NON-NLS-1$                            
                            "<symbol>nis_element2_attribute2</symbol>"+ //$NON-NLS-1$                            
                        "</mappingNode>" + //$NON-NLS-1$
                    "</mappingNode>" + //$NON-NLS-1$
                "</mappingNode>" + //$NON-NLS-1$
                "</xmlMapping>"; //$NON-NLS-1$
        
        MappingOutputter out = new MappingOutputter();
        StringWriter sw = new StringWriter();
        out.write(doc, new PrintWriter(sw));
        
        assertEquals(expected, sw.toString());
    }
}
