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

package org.teiid.query.sql.visitor;

import java.util.Collection;
import java.util.Iterator;

import org.teiid.core.TeiidException;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;

import junit.framework.TestCase;


public class TestDeepGroupCollectorVisitor extends TestCase {
    public TestDeepGroupCollectorVisitor(String name) {
        super(name);
    }

    public void helpTestVisitor(String sql, String[] expectedGroups) {
        LanguageObject obj = null;
        try {
            QueryParser parser = new QueryParser();
            obj = parser.parseCommand(sql);
        } catch(TeiidException e) {
            fail("Unexpected exception while parsing: " + e.getFullMessage()); //$NON-NLS-1$
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
