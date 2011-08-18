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

import junit.framework.TestCase;

import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.processor.xml.TestXMLProcessor;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 */
public class TestXMLNodeMappingVisitor extends TestCase {

    /**
     * Constructor for TestXMLNodeMappingVisitor.
     * @param arg0
     */
    public TestXMLNodeMappingVisitor(String arg0) {
        super(arg0);
    }

	public void helpTestMapping(Criteria crit, String expectedCritString, MappingDocument mappingDoc, QueryMetadataInterface metadata) throws QueryPlannerException, TeiidComponentException {
        crit = XMLNodeMappingVisitor.convertCriteria(crit, mappingDoc, metadata);
		String actualCritString = crit.toString();
		
		assertEquals("Got incorrect converted string: ", expectedCritString, actualCritString); //$NON-NLS-1$
	}	
			
	public void testMappingCriteria() throws Exception {
        QueryMetadataInterface metadata = TestXMLProcessor.exampleMetadata();

        GroupSymbol doc = new GroupSymbol("xmltest.doc1"); //$NON-NLS-1$
        doc.setMetadataID(metadata.getGroupID(doc.getName()));
            
        MappingDocument mappingDoc = (MappingDocument)metadata.getMappingNode(doc.getMetadataID());
        mappingDoc = SourceNodeGenaratorVisitor.extractSourceNodes(mappingDoc);
    
		// Create criteria
       	ElementSymbol es = new ElementSymbol("Catalogs.Catalog.Items.Item.Name", null, doc); //$NON-NLS-1$
        ResolverVisitor.resolveLanguageObject(es, metadata);
		CompareCriteria crit = new CompareCriteria(es, CompareCriteria.EQ, new Constant("abc")); //$NON-NLS-1$
	
		helpTestMapping(crit, "xmltest.\"group\".items.itemName = 'abc'", mappingDoc, metadata); //$NON-NLS-1$
	}

}
