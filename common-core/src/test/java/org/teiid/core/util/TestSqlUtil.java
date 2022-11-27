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

package org.teiid.core.util;

import static org.junit.Assert.*;

import org.junit.Test;
/**
 */
public class TestSqlUtil {

    public void helpTest(String sql, boolean isUpdate) {
        boolean actual = SqlUtil.isUpdateSql(sql);
        assertEquals(isUpdate, actual);
    }

    @Test public void testSelect() {
        helpTest("SELECT x FROM y", false); //$NON-NLS-1$
    }

    @Test public void testInsert() {
        helpTest("Insert INTO g (a) VALUES (1)", true); //$NON-NLS-1$
    }

    @Test public void testUpdate() {
        helpTest("upDate x set a=5", true); //$NON-NLS-1$
    }

    @Test public void testDelete() {
        helpTest("delete FROM x", true); //$NON-NLS-1$
    }

    @Test public void testInsertWithWhitespace() {
        helpTest("\nINSERT INTO g (a) VALUES (1)", true); //$NON-NLS-1$
    }

    @Test public void testExec() {
        helpTest("exec sq1()", false); //$NON-NLS-1$
    }

    @Test public void testXquery() {
        helpTest("<i/>", false); //$NON-NLS-1$
    }

    @Test public void testSelectInto1() {
        helpTest("SELECT x INTO z FROM y", true); //$NON-NLS-1$
    }

    @Test public void testSelectInto2() {
        helpTest("SELECT x, INTOz FROM y", false); //$NON-NLS-1$
    }

    @Test public void testSelectInto3() {
        helpTest("SELECT x into z FROM y", true); //$NON-NLS-1$
    }

    @Test public void testSelectInto4() {
        helpTest("SELECT x into z", true); //$NON-NLS-1$
    }

    @Test public void testSelectInto5() {
        helpTest("SELECT x, ' into ' from z", false); //$NON-NLS-1$
    }

    @Test public void testCreate() {
        helpTest(" create table x", true); //$NON-NLS-1$
    }

    @Test public void testDrop() {
        helpTest("/* */ drop table x", true); //$NON-NLS-1$
    }

}
