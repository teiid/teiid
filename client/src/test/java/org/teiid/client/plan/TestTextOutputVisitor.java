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

package org.teiid.client.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.teiid.client.plan.DefaultDisplayHelper;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.plan.TextOutputVisitor;

import junit.framework.TestCase;


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
    	HashMap<String, Object> map = new HashMap<String, Object>();
    	map.put(PlanNode.TYPE, "x"); //$NON-NLS-1$ 
    	map.put("test", ""); //$NON-NLS-1$ //$NON-NLS-2$
    	map.put("string", "string"); //$NON-NLS-1$ //$NON-NLS-2$
    	map.put("integer", new Integer(0)); //$NON-NLS-1$
    	map.put("boolean", Boolean.TRUE); //$NON-NLS-1$
        List list1 = new ArrayList();
        list1.add("item1"); //$NON-NLS-1$
        list1.add("item2"); //$NON-NLS-1$
        list1.add("item3"); //$NON-NLS-1$
        map.put("list<string>", list1); //$NON-NLS-1$
        
        HashMap<String, Object> child = new HashMap<String, Object>();
        child.put(PlanNode.TYPE, "y"); //$NON-NLS-1$
        List<String> outputCols = new ArrayList<String>();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        child.put(PlanNode.OUTPUT_COLS, outputCols);
        child.put("Join Type", "INNER JOIN"); //$NON-NLS-1$ //$NON-NLS-2$
        List<String> crits = new ArrayList<String>();
        crits.add("Item.ID = History.ID"); //$NON-NLS-1$
        child.put("Criteria", crits); //$NON-NLS-1$
        
        map.put(PlanNode.PROP_CHILDREN, Arrays.asList(child));
        
        return PlanNode.constructFromMap(map);
    }
        
    public void testWithDefaultDisplayHelper() {
        TextOutputVisitor v = new TextOutputVisitor(new DefaultDisplayHelper(), 0);
        v.visit(example1());
        assertEquals("x\n  + test: \n  + integer: 0\n  + string: string\n  + list<string>:\n      1: item1\n      2: item2\n      3: item3\n  + boolean: true\n  y\n    + outputCols:\n        1: Name (string)\n        2: Year (integer)\n    + Join Type: INNER JOIN\n    + Criteria:\n        1: Item.ID = History.ID\n", v.getText()); //$NON-NLS-1$
    }
    
}
