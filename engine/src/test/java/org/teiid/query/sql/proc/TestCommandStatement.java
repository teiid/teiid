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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.proc.CommandStatement;

import junit.framework.*;

/**
 *
 * @author gchadalavadaDec 9, 2002
 */
public class TestCommandStatement  extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestCommandStatement(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static final CommandStatement sample1() {
        QueryParser parser = new QueryParser();
        try {
            Query query = (Query) parser.parseCommand("Select x from y"); //$NON-NLS-1$
            return new CommandStatement(query);
        } catch(Exception e) { return null;}
    }

    public static final CommandStatement sample2() {
        QueryParser parser = new QueryParser();
        try {
            Update update = (Update) parser.parseCommand("UPDATE x SET x = 'y'"); //$NON-NLS-1$
            return new CommandStatement(update);
        } catch(Exception e) { return null;}
    }

    // ################################## ACTUAL TESTS ################################

    public void testSelfEquivalence(){
        CommandStatement s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        CommandStatement s1 = sample1();
        CommandStatement s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testNonEquivalence(){
        CommandStatement s1 = sample1();
        CommandStatement s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }
}
