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

package org.teiid.query.sql.visitor;

import java.util.Collection;
import java.util.Iterator;

import junit.framework.TestCase;

import org.teiid.core.TeiidException;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.symbol.GroupSymbol;


public class TestDeepGroupCollectorVisitor extends TestCase {
    public TestDeepGroupCollectorVisitor(String name) {
        super(name);
    }

    public void helpTestVisitor(String sql, String[] expectedGroups) {
        LanguageObject obj = null;
        try {
            obj = QueryParser.getQueryParser().parseCommand(sql);
        } catch(TeiidException e) {
            throw new RuntimeException(e);
        }

        Collection actualGroups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(obj, false);
        assertEquals("Did not get expected number of groups", expectedGroups.length, actualGroups.size()); //$NON-NLS-1$

        Iterator iter = actualGroups.iterator();
        for(int i=0; iter.hasNext(); i++) {
            GroupSymbol group = (GroupSymbol) iter.next();
            assertTrue("Expected group did not match, expected=" + expectedGroups[i] + ", actual=" + group,  //$NON-NLS-1$ //$NON-NLS-2$
                group.getName().equalsIgnoreCase(expectedGroups[i]));
        }
    }

    public void testQuery1() {
        helpTestVisitor("SELECT * FROM pm1.g1",  //$NON-NLS-1$
            new String[] { "pm1.g1" });     //$NON-NLS-1$
    }

    public void testSubquery1() {
        helpTestVisitor("SELECT * FROM (SELECT * FROM pm1.g1) AS x",  //$NON-NLS-1$
            new String[] { "pm1.g1" });             //$NON-NLS-1$
    }

    public void testSubquery2() {
        helpTestVisitor("SELECT * FROM (SELECT * FROM pm1.g1) AS x, pm1.g2",  //$NON-NLS-1$
            new String[] { "pm1.g1", "pm1.g2"});             //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubquery3() {
        helpTestVisitor("SELECT * FROM pm1.g2 WHERE e1 IN (SELECT * FROM pm1.g1)",  //$NON-NLS-1$
            new String[] { "pm1.g2", "pm1.g1"});             //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubquery4() {
        helpTestVisitor("SELECT * FROM pm1.g2 WHERE e1 IN (SELECT * FROM pm1.g1, (SELECT * FROM pm1.g3) AS x)",  //$NON-NLS-1$
            new String[] { "pm1.g2", "pm1.g1", "pm1.g3" });             //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testExec1() {
        helpTestVisitor("EXEC pm1.sq1()",  //$NON-NLS-1$
            new String[] { "pm1.sq1" });             //$NON-NLS-1$
    }

    public void testSubqueryExec1() {
        helpTestVisitor("SELECT * FROM (EXEC pm1.sq1()) AS x",  //$NON-NLS-1$
            new String[] { "pm1.sq1" });             //$NON-NLS-1$
    }

    public void testUnionInSubquery() {
        helpTestVisitor("SELECT x.intkey FROM (SELECT intkey FROM BQT1.SmallA UNION SELECT intkey FROM BQT1.SmallB) AS x", //$NON-NLS-1$
            new String[] { "BQT1.SmallA", "BQT1.SmallB"} );     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUnionOfSubquery() {
        helpTestVisitor("SELECT x.intkey FROM (SELECT intkey FROM BQT1.SmallA) AS x UNION SELECT intkey FROM BQT1.SmallB", //$NON-NLS-1$
            new String[] { "BQT1.SmallA", "BQT1.SmallB"} );     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSubqueryInCriteria() {
        helpTestVisitor("SELECT * FROM BQT1.SmallA WHERE intkey IN (SELECT intkey FROM BQT1.SmallB)", //$NON-NLS-1$
            new String[] { "BQT1.SmallA", "BQT1.SmallB"} );     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUnionSubqueryInCriteria() {
        helpTestVisitor("SELECT * FROM BQT1.SmallA WHERE intkey IN (SELECT intkey FROM BQT1.SmallB UNION SELECT intkey FROM BQT2.SmallA)", //$NON-NLS-1$
            new String[] { "BQT1.SmallA", "BQT1.SmallB", "BQT2.SmallA" } );     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

}
