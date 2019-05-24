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

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;


/**
 *
 * @author gchadalavadaDec 9, 2002
 */
public class TestCreateUpdateProcedureCommand {

    // ################################## TEST HELPERS ################################

    public static final CreateProcedureCommand sample1() {
        return new CreateProcedureCommand(TestBlock.sample1());
    }

    public static final CreateProcedureCommand sample2() {
        return new CreateProcedureCommand(TestBlock.sample2());
    }

    public static final CreateProcedureCommand sample3() {
        return new CreateProcedureCommand(TestBlock.sample1());
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void testGetBlock() {
        CreateProcedureCommand b1 = sample1();
        assertTrue("Incorrect Block on command", b1.getBlock().equals(TestBlock.sample1())); //$NON-NLS-1$
    }

    public void testSetBlock() {
        CreateProcedureCommand b1 = (CreateProcedureCommand)sample1().clone();
        b1.setBlock(TestBlock.sample2());
        assertTrue("Incorrect Block on command", b1.getBlock().equals(TestBlock.sample2())); //$NON-NLS-1$
    }

    public void testSelfEquivalence(){
        CreateProcedureCommand s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        CreateProcedureCommand s1 = sample1();
        CreateProcedureCommand s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testNonEquivalence(){
        CreateProcedureCommand s1 = sample1();
        CreateProcedureCommand s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

    /**
     * Test cloning obj with mostly null state, test for NPE
     */
    public void testCloneMethod3() {
        CreateProcedureCommand s1 = new CreateProcedureCommand();
        CreateProcedureCommand s2 = (CreateProcedureCommand)s1.clone();
        UnitTestUtil.helpTestEquivalence(0, s1, s2);
    }

}
