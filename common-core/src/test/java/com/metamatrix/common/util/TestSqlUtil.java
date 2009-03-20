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

package com.metamatrix.common.util;

import junit.framework.TestCase;

/**
 */
public class TestSqlUtil extends TestCase {

    public TestSqlUtil(String name) {
        super(name);
    }

    public void helpTest(String sql, boolean isUpdate) {
        boolean actual = SqlUtil.isUpdateSql(sql);
        assertEquals(isUpdate, actual);
    }
    
    public void testSelect() {
        helpTest("SELECT x FROM y", false); //$NON-NLS-1$
    }
    
    public void testInsert() {
        helpTest("Insert INTO g (a) VALUES (1)", true); //$NON-NLS-1$
    }
    
    public void testUpdate() {
        helpTest("upDate x set a=5", true); //$NON-NLS-1$
    }
    
    public void testDelete() {
        helpTest("delete FROM x", true); //$NON-NLS-1$
    }

    public void testInsertWithWhitespace() {
        helpTest("\nINSERT INTO g (a) VALUES (1)", true); //$NON-NLS-1$
    }
    
    public void testExec() {
        helpTest("exec sq1()", false); //$NON-NLS-1$
    }
    
    public void testXquery() {
        helpTest("<i/>", false); //$NON-NLS-1$
    }
    
    public void testSelectInto1() {
        helpTest("SELECT x INTO z FROM y", true); //$NON-NLS-1$
    }
    
    public void testSelectInto2() {
        helpTest("SELECT x, INTOz FROM y", false); //$NON-NLS-1$
    }
    
    public void testSelectInto3() {
        helpTest("SELECT x into z FROM y", true); //$NON-NLS-1$
    }

    public void testSelectInto4() {
        helpTest("SELECT x into z", true); //$NON-NLS-1$
    }
    
    public void testCreate() {
    	helpTest(" create table x", true); //$NON-NLS-1$
    }
    
    public void testDrop() {
    	helpTest("/* */ drop table x", true); //$NON-NLS-1$
    }

}
