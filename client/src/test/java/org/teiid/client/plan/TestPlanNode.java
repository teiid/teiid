/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.client.plan;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;


/**
 */
@SuppressWarnings("nls")
public class TestPlanNode {

    public static PlanNode example1() {
        PlanNode map = new PlanNode("x"); //$NON-NLS-1$
        map.addProperty("test", ""); //$NON-NLS-1$ //$NON-NLS-2$
        map.addProperty("null", (String)null); //$NON-NLS-1$
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
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><node name=\"x\"><property name=\"test\"><value></value></property><property name=\"null\"></property><property name=\"string\"><value>string</value></property><property name=\"list&lt;string&gt;\"><value>item1</value><value>item2</value><value>item3</value></property><property name=\"child\"><node name=\"y\"><property name=\"outputCols\"><value>Name (string)</value><value>Year (integer)</value></property><property name=\"Join Type\"><value>INNER JOIN</value></property><property name=\"Criteria\"><value>Item.ID = History.ID</value></property><property name=\"Other\"></property></node></property></node>", example1().toXml()); //$NON-NLS-1$
    }

    @Test public void testXmlRoundtrip() throws Exception {
        PlanNode example1 = example1();
        example1.addProperty("last", "x"); //$NON-NLS-1$ //$NON-NLS-2$
        String planString = example1.toXml();
        PlanNode planNode = PlanNode.fromXml(planString);
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><node name=\"x\"><property name=\"test\"><value></value></property><property name=\"null\"></property><property name=\"string\"><value>string</value></property><property name=\"list&lt;string&gt;\"><value>item1</value><value>item2</value><value>item3</value></property><property name=\"child\"><node name=\"y\"><property name=\"outputCols\"><value>Name (string)</value><value>Year (integer)</value></property><property name=\"Join Type\"><value>INNER JOIN</value></property><property name=\"Criteria\"><value>Item.ID = History.ID</value></property><property name=\"Other\"></property></node></property><property name=\"last\"><value>x</value></property></node>", planNode.toXml()); //$NON-NLS-1$
    }

    @Test public void testText() throws Exception {
        assertEquals("x\n  + test:\n  + null\n  + string:string\n  + list<string>:\n    0: item1\n    1: item2\n    2: item3\n  + child:\n    y\n      + outputCols:\n        0: Name (string)\n        1: Year (integer)\n      + Join Type:INNER JOIN\n      + Criteria:Item.ID = History.ID\n      + Other\n", example1().toString()); //$NON-NLS-1$
    }

    @Test public void testYaml() throws Exception {
        assertEquals("x:\n" +
                "  test: \n" +
                "  null: ~\n" +
                "  string: string\n" +
                "  list<string>:\n" +
                "    - item1\n" +
                "    - item2\n" +
                "    - item3\n" +
                "  child:\n" +
                "    y:\n" +
                "      outputCols:\n" +
                "        - Name (string)\n" +
                "        - Year (integer)\n" +
                "      Join Type: INNER JOIN\n" +
                "      Criteria: Item.ID = History.ID\n" +
                "      Other: ~\n", example1().toYaml());
    }

}
