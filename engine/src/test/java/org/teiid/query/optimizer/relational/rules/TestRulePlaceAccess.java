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

package org.teiid.query.optimizer.relational.rules;

import java.util.Collection;

import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.AllSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.unittest.RealMetadataFactory;

import junit.framework.TestCase;


public class TestRulePlaceAccess extends TestCase {

    private static final QueryMetadataInterface METADATA = RealMetadataFactory.example1Cached();

    // ################################## FRAMEWORK ################################

    /**
     * Constructor for TestRulePlaceAccess.
     * @param name
     */
    public TestRulePlaceAccess(String name) {
        super(name);
    }

    /**
     * Tests that any access patterns (a Collection of Collections of
     * Object element ids) for a physical group will be found and added
     * as a property of an ACCESS node.
     */
    public void testAddAccessPatterns2() throws Exception {
        Query query = new Query();
        
        From from = new From();
        GroupSymbol group = new GroupSymbol("pm4.g2"); //$NON-NLS-1$
        from.addGroup(group);
        query.setFrom(from);
        
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);

        group.setMetadataID(METADATA.getGroupID("pm4.g2")); //$NON-NLS-1$
                
        PlanNode n1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        n1.setProperty(NodeConstants.Info.ATOMIC_REQUEST, query);
        n1.addGroup(group);
        
        RulePlaceAccess.addAccessPatternsProperty(n1, METADATA);

        Collection accessPatterns = (Collection)n1.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
        assertNotNull(accessPatterns);
        assertTrue("Expected two access patterns, got " + accessPatterns.size(), accessPatterns.size() == 2); //$NON-NLS-1$
    }   
    

}
