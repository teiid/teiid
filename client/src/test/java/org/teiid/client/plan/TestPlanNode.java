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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;


/**
 */
public class TestPlanNode {

    public static PlanNode example1() {
    	PlanNode map = new PlanNode("x"); //$NON-NLS-1$ 
    	map.addProperty("test", ""); //$NON-NLS-1$ //$NON-NLS-2$
    	map.addProperty("string", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        List<String> list1 = new ArrayList<String>();
        list1.add("item1"); //$NON-NLS-1$
        list1.add("item2"); //$NON-NLS-1$
        list1.add("item3"); //$NON-NLS-1$
        map.addProperty("list<string>", list1); //$NON-NLS-1$
        
        PlanNode child = new PlanNode("y"); //$NON-NLS-1$
        List<String> outputCols = new ArrayList<String>();
        outputCols.add("Name (string)"); //$NON-NLS-1$
        outputCols.add("Year (integer)"); //$NON-NLS-1$
        child.addProperty("outputCols", outputCols); //$NON-NLS-1$
        child.addProperty("Join Type", "INNER JOIN"); //$NON-NLS-1$ //$NON-NLS-2$
        List<String> crits = new ArrayList<String>();
        crits.add("Item.ID = History.ID"); //$NON-NLS-1$
        child.addProperty("Criteria", crits); //$NON-NLS-1$
        child.addProperty("Other", new ArrayList<String>()); //$NON-NLS-1$
        map.addProperty("child", child); //$NON-NLS-1$
        return map;
    }

    @Test public void testXml() throws Exception {
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<node name=\"x\">\n    <property name=\"test\">\n        <value></value>\n    </property>\n    <property name=\"string\">\n        <value>string</value>\n    </property>\n    <property name=\"list&lt;string&gt;\">\n        <value>item1</value>\n        <value>item2</value>\n        <value>item3</value>\n    </property>\n    <property name=\"child\">\n        <node name=\"y\">\n            <property name=\"outputCols\">\n                <value>Name (string)</value>\n                <value>Year (integer)</value>\n            </property>\n            <property name=\"Join Type\">\n                <value>INNER JOIN</value>\n            </property>\n            <property name=\"Criteria\">\n                <value>Item.ID = History.ID</value>\n            </property>\n            <property name=\"Other\"/>\n        </node>\n    </property>\n</node>\n", example1().toXml()); //$NON-NLS-1$
    }
    
    @Test public void testXmlRoundtrip() throws Exception {
    	String planString = example1().toXml();
    	PlanNode planNode = PlanNode.fromXml(planString);
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<node name=\"x\">\n    <property name=\"test\">\n        <value></value>\n    </property>\n    <property name=\"string\">\n        <value>string</value>\n    </property>\n    <property name=\"list&lt;string&gt;\">\n        <value>item1</value>\n        <value>item2</value>\n        <value>item3</value>\n    </property>\n    <property name=\"child\">\n        <node name=\"y\">\n            <property name=\"outputCols\">\n                <value>Name (string)</value>\n                <value>Year (integer)</value>\n            </property>\n            <property name=\"Join Type\">\n                <value>INNER JOIN</value>\n            </property>\n            <property name=\"Criteria\">\n                <value>Item.ID = History.ID</value>\n            </property>\n            <property name=\"Other\"/>\n        </node>\n    </property>\n</node>\n", planNode.toXml()); //$NON-NLS-1$
    }

    @Test public void testText() throws Exception {
        assertEquals("x\n  + test:\n  + string:string\n  + list<string>:\n    0: item1\n    1: item2\n    2: item3\n  + child:\n    y\n      + outputCols:\n        0: Name (string)\n        1: Year (integer)\n      + Join Type:INNER JOIN\n      + Criteria:Item.ID = History.ID\n      + Other\n", example1().toString()); //$NON-NLS-1$
    }
    
}
