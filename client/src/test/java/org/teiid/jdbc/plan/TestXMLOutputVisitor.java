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

package org.teiid.jdbc.plan;

import org.teiid.client.plan.DefaultDisplayHelper;
import org.teiid.client.plan.XMLOutputVisitor;

import junit.framework.TestCase;


/**
 */
public class TestXMLOutputVisitor extends TestCase {

    public TestXMLOutputVisitor(String name) {
        super(name);
    }

    public void testWithDefaultDisplayHelper() {
        XMLOutputVisitor v = new XMLOutputVisitor(new DefaultDisplayHelper());
        v.visit(TestTextOutputVisitor.example1());
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<node name=\"x\">\n  <properties>\n    <property name=\"test\" value=\"\"/>\n    <property name=\"integer\" value=\"0\"/>\n    <property name=\"string\" value=\"string\"/>\n    <property name=\"list&lt;string&gt;\">\n      <collection>\n        <value>item1</value>\n        <value>item2</value>\n        <value>item3</value>\n      </collection>\n    </property>\n    <property name=\"boolean\" value=\"true\"/>\n  </properties>\n  <node name=\"y\">\n    <properties>\n      <property name=\"outputCols\">\n        <collection>\n          <value>Name (string)</value>\n          <value>Year (integer)</value>\n        </collection>\n      </property>\n      <property name=\"Join Type\" value=\"INNER JOIN\"/>\n      <property name=\"Criteria\">\n        <collection>\n          <value>Item.ID = History.ID</value>\n        </collection>\n      </property>\n    </properties>\n  </node>\n</node>\n", v.getText()); //$NON-NLS-1$
    }

}
