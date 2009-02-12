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

package com.metamatrix.jdbc.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.jdbc.api.PlanNode;
import com.metamatrix.jdbc.api.XMLOutputVisitor;
import com.metamatrix.jdbc.api.tools.QueryPlanDisplayHelper;

/**
 */
public class TestXMLOutputVisitor extends TestCase {

    /**
     * Constructor for TestXMLOutputVisitor.
     * @param name
     */
    public TestXMLOutputVisitor(String name) {
        super(name);
    }

    public void testTypicalExample() {
        PlanNode plan = TestTextOutputVisitor.example1();
        XMLOutputVisitor v = new XMLOutputVisitor(new FakeDisplayHelper());
        v.visit(plan);
    }
    
    public void testCommonTypes() {
        PlanNode plan = TestTextOutputVisitor.example2();
        XMLOutputVisitor v = new XMLOutputVisitor(new FakeDisplayHelper());
        v.visit(plan);
    } 
    
    public void testNestedNode() {
        PlanNode plan = TestTextOutputVisitor.example2();
        plan.getProperties().put("nested", TestTextOutputVisitor.example1()); //$NON-NLS-1$
        XMLOutputVisitor v = new XMLOutputVisitor(new FakeDisplayHelper());
        v.visit(plan);
    }    

    public void testLimitNode() {
        FakePlanNode node = new FakePlanNode("Limit", ""); //$NON-NLS-1$ //$NON-NLS-2$
        List outputCols = new ArrayList();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        outputCols.add("Age (integer)"); //$NON-NLS-1$
        node.setProperty(FakePlanNode.PROP_OUTPUT_COLS, outputCols);
        node.setProperty("rowLimit", "100"); //$NON-NLS-1$ //$NON-NLS-2$
        XMLOutputVisitor v = new XMLOutputVisitor(new QueryPlanDisplayHelper());
        v.visit(node);
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<node name=\"Limit [100]\">\n  <properties>\n    <property name=\"Output Columns\">\n      <collection>\n        <value>Name (string)</value>\n        <value>Year (integer)</value>\n        <value>Age (integer)</value>\n      </collection>\n    </property>\n    <property name=\"Row Limit\" value=\"100\"/>\n  </properties>\n</node>\n", v.getText()); //$NON-NLS-1$
    }
    public void testOffsetNode() {
        FakePlanNode node = new FakePlanNode("Offset", ""); //$NON-NLS-1$ //$NON-NLS-2$
        List outputCols = new ArrayList();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        outputCols.add("Age (integer)"); //$NON-NLS-1$
        node.setProperty(FakePlanNode.PROP_OUTPUT_COLS, outputCols);
        node.setProperty("rowOffset", "100"); //$NON-NLS-1$ //$NON-NLS-2$
        XMLOutputVisitor v = new XMLOutputVisitor(new QueryPlanDisplayHelper());
        v.visit(node);
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<node name=\"Offset [100]\">\n  <properties>\n    <property name=\"Output Columns\">\n      <collection>\n        <value>Name (string)</value>\n        <value>Year (integer)</value>\n        <value>Age (integer)</value>\n      </collection>\n    </property>\n    <property name=\"Row Offset\" value=\"100\"/>\n  </properties>\n</node>\n", v.getText()); //$NON-NLS-1$
    }
}
