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
import com.metamatrix.jdbc.api.TextOutputVisitor;
import com.metamatrix.jdbc.api.tools.QueryPlanDisplayHelper;

/**
 */
public class TestTextOutputVisitor extends TestCase {

    /**
     * Constructor for TestTextOutputVisitor.
     * @param name
     */
    public TestTextOutputVisitor(String name) {
        super(name);
    }

    public static PlanNode example1() {
        FakePlanNode n1 = new FakePlanNode("Relational Plan", ""); //$NON-NLS-1$ //$NON-NLS-2$
        List outputCols = new ArrayList();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        outputCols.add("Age (integer)"); //$NON-NLS-1$
        n1.setProperty(FakePlanNode.PROP_OUTPUT_COLS, outputCols);
        
        FakePlanNode n2 = new FakePlanNode("Project", "Name, Year, YEAR(CURDATE()) - year AS Age"); //$NON-NLS-1$ //$NON-NLS-2$
        outputCols = new ArrayList();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        n2.setProperty(FakePlanNode.PROP_OUTPUT_COLS, outputCols);

        FakePlanNode n3 = new FakePlanNode("Join", "Item JOIN History"); //$NON-NLS-1$ //$NON-NLS-2$
        outputCols = new ArrayList();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        n3.setProperty(FakePlanNode.PROP_OUTPUT_COLS, outputCols);
        n3.setProperty("Join Type", "INNER JOIN"); //$NON-NLS-1$ //$NON-NLS-2$
        List crits = new ArrayList();
        crits.add("Item.ID = History.ID"); //$NON-NLS-1$
        n3.setProperty("Criteria", crits); //$NON-NLS-1$
        
        connectNodes(n1,n2);
        connectNodes(n2,n3);
                
        return n1;
    }

    public static PlanNode example2() {
        FakePlanNode n = new FakePlanNode("test", ""); //$NON-NLS-1$ //$NON-NLS-2$
        n.setProperty("string", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        n.setProperty("integer", new Integer(0)); //$NON-NLS-1$
        n.setProperty("boolean", Boolean.TRUE); //$NON-NLS-1$
        List list1 = new ArrayList();
        list1.add("item1"); //$NON-NLS-1$
        list1.add("item2"); //$NON-NLS-1$
        list1.add("item3"); //$NON-NLS-1$
        n.setProperty("list<string>", list1); //$NON-NLS-1$
        
        return n;
    }
        
    public static void connectNodes(FakePlanNode parent, FakePlanNode child) {
        parent.addChild(child);
        child.setParent(parent);        
    }

    public void testTypicalExample() {
        PlanNode plan = example1();
        TextOutputVisitor v = new TextOutputVisitor(new FakeDisplayHelper(), 0);
        v.visit(plan);
    }
    
    public void testCommonTypes() {
        PlanNode plan = example2();
        TextOutputVisitor v = new TextOutputVisitor(new FakeDisplayHelper(), 0);
        v.visit(plan);
    } 
    
    public void testNestedNode() {
        PlanNode plan = example2();
        plan.getProperties().put("nested", example1()); //$NON-NLS-1$
        TextOutputVisitor v = new TextOutputVisitor(new FakeDisplayHelper(), 0);
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
        TextOutputVisitor v = new TextOutputVisitor(new QueryPlanDisplayHelper(), 0);
        v.visit(node);
        assertEquals("Limit [100]\n  + Output Columns:\n      1: Name (string)\n      2: Year (integer)\n      3: Age (integer)\n  + Row Limit: 100\n", v.getText()); //$NON-NLS-1$
    }
    
    public void testOffsetNode() {
        FakePlanNode node = new FakePlanNode("Offset", ""); //$NON-NLS-1$ //$NON-NLS-2$
        List outputCols = new ArrayList();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        outputCols.add("Age (integer)"); //$NON-NLS-1$
        node.setProperty(FakePlanNode.PROP_OUTPUT_COLS, outputCols);
        node.setProperty("rowOffset", "100"); //$NON-NLS-1$ //$NON-NLS-2$
        TextOutputVisitor v = new TextOutputVisitor(new QueryPlanDisplayHelper(), 0);
        v.visit(node);
        assertEquals("Offset [100]\n  + Output Columns:\n      1: Name (string)\n      2: Year (integer)\n      3: Age (integer)\n  + Row Offset: 100\n", v.getText()); //$NON-NLS-1$
    }
       
}
