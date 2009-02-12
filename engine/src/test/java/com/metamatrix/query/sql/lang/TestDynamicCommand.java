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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;

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
