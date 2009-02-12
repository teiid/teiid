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

package com.metamatrix.query.optimizer.xml;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.mapping.xml.MappingDocument;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.processor.xml.TestXMLProcessor;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.unittest.FakeMetadataFacade;

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

	public void helpTestMapping(Criteria crit, String expectedCritString, MappingDocument mappingDoc, QueryMetadataInterface metadata) throws QueryPlannerException, MetaMatrixComponentException {
        crit = XMLNodeMappingVisitor.convertCriteria(crit, mappingDoc, metadata);
		String actualCritString = crit.toString();
		
		assertEquals("Got incorrect converted string: ", expectedCritString, actualCritString); //$NON-NLS-1$
	}	
			
	public void testMappingCriteria() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata();

        GroupSymbol doc = new GroupSymbol("xmltest.doc1"); //$NON-NLS-1$
        doc.setMetadataID(metadata.getGroupID(doc.getName()));
            

        MappingDocument mappingDoc = (MappingDocument)metadata.getMappingNode(doc.getMetadataID());
        mappingDoc = SourceNodeGenaratorVisitor.extractSourceNodes(mappingDoc);
    
		// Create criteria
       	ElementSymbol es = new ElementSymbol("Catalogs.Catalog.Items.Item.Name"); //$NON-NLS-1$
        es.setGroupSymbol(doc);
        es.setMetadataID(metadata.getElementID("xmltest.doc1.Catalogs.Catalog.Items.Item.Name")); //$NON-NLS-1$
		CompareCriteria crit = new CompareCriteria(es, CompareCriteria.EQ, new Constant("abc")); //$NON-NLS-1$
	
		helpTestMapping(crit, "xmltest.\"group\".items.itemName = 'abc'", mappingDoc, metadata); //$NON-NLS-1$
	}

}
