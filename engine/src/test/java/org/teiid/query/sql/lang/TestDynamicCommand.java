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

package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


/**
 * Test for DynamicCommand
 */
public class TestDynamicCommand extends TestCase {

    /**
     * Constructor for TestDynamicCommand.
     * @param name
     */
    public TestDynamicCommand(String name) {
        super(name);
    }

    public void testClone1() {
        List symbols = new ArrayList();

        ElementSymbol a1 = new ElementSymbol("a1"); //$NON-NLS-1$
        a1.setType(DataTypeManager.DefaultDataClasses.STRING);
        symbols.add(a1);

        DynamicCommand sqlCmd = new DynamicCommand();
        Expression sql = new Constant("SELECT a1 FROM g WHERE a2 = 5"); //$NON-NLS-1$

        sqlCmd.setSql(sql);
        sqlCmd.setAsColumns(symbols);
        sqlCmd.setAsClauseSet(true);

        sqlCmd.setIntoGroup(new GroupSymbol("#g")); //$NON-NLS-1$

        UnitTestUtil.helpTestEquivalence(0, sqlCmd, sqlCmd.clone());
    }

    public void testClone2() {
        List symbols = new ArrayList();

        ElementSymbol a1 = new ElementSymbol("a1"); //$NON-NLS-1$
        a1.setType(DataTypeManager.DefaultDataClasses.STRING);
        symbols.add(a1);
        Expression sql = new Constant("SELECT * FROM g"); //$NON-NLS-1$

        SetClauseList using = new SetClauseList();
        using.addClause(a1, a1);

        DynamicCommand sqlCmd = new DynamicCommand(sql, symbols, new GroupSymbol("#g"), using); //$NON-NLS-1$

        UnitTestUtil.helpTestEquivalence(0, sqlCmd, sqlCmd.clone());
    }

    public void testClone3() {

        List symbols = new ArrayList();

        ElementSymbol a1 = new ElementSymbol("a1"); //$NON-NLS-1$
        a1.setType(DataTypeManager.DefaultDataClasses.STRING);
        symbols.add(a1);

        DynamicCommand sqlCmd = new DynamicCommand();
        Expression sql = new Constant("SELECT a1 FROM g WHERE a2 = 5"); //$NON-NLS-1$

        sqlCmd.setSql(sql);
        sqlCmd.setAsColumns(symbols);
        sqlCmd.setAsClauseSet(true);

        sqlCmd.setIntoGroup(new GroupSymbol("#g")); //$NON-NLS-1$

        List projectedSymbols = sqlCmd.getProjectedSymbols();

        UnitTestUtil.helpTestEquivalence(0, sqlCmd, sqlCmd.clone());
        assertEquals(projectedSymbols, ((DynamicCommand)sqlCmd.clone()).getProjectedSymbols());
    }

    public void testUpdatingModelCount() {
        DynamicCommand sqlCmd = new DynamicCommand();

        sqlCmd.setUpdatingModelCount(1);
        assertEquals(1, sqlCmd.getUpdatingModelCount());

        sqlCmd.setUpdatingModelCount(3);
        assertEquals(2, sqlCmd.getUpdatingModelCount());

        sqlCmd.setUpdatingModelCount(-1);
        assertEquals(0, sqlCmd.getUpdatingModelCount());
    }

}
