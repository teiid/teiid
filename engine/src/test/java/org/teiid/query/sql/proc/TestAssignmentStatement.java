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

package org.teiid.query.sql.proc;

import java.util.Arrays;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;

import junit.framework.TestCase;


/**
 *
 * @author gchadalavadaDec 11, 2002
 */
public class TestAssignmentStatement  extends TestCase {

    /**
     * Constructor for TestAssignmentStatement.
     */
    public TestAssignmentStatement(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static final AssignmentStatement sample1() {
        return new AssignmentStatement(new ElementSymbol("a"), new Constant("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static final AssignmentStatement sample2() {
        Query query = new Query();
        query.setSelect(new Select(Arrays.asList(new ElementSymbol("x")))); //$NON-NLS-1$
        query.setFrom(new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("y"))))); //$NON-NLS-1$
        return new AssignmentStatement(new ElementSymbol("b"), query); //$NON-NLS-1$
    }

    // ################################## ACTUAL TESTS ################################

    public void testGetVariable() {
        AssignmentStatement s1 = sample1();
        assertEquals("Didn't get the same parts ", s1.getVariable(), new ElementSymbol("a"));         //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetExpression() {
        AssignmentStatement s1 = sample1();
        assertEquals("Didn't get the same parts ", s1.getExpression(), new Constant("1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetCommand() throws Exception {
        AssignmentStatement s2 = sample2();
        Query query = (Query) QueryParser.getQueryParser().parseCommand("Select x from y"); //$NON-NLS-1$
        assertEquals("Didn't get the same parts ", ((ScalarSubquery)s2.getExpression()).getCommand(), query); //$NON-NLS-1$
    }

    public void testSelfEquivalence(){
        AssignmentStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        AssignmentStatement s1 = sample1();
        AssignmentStatement s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testNonEquivalence(){
        AssignmentStatement s1 = sample1();
        AssignmentStatement s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

}
