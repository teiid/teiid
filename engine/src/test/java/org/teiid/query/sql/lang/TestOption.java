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

package org.teiid.query.sql.lang;

import java.util.Arrays;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.Option;

import junit.framework.TestCase;


/**
 */
public class TestOption extends TestCase {

    /**
     * Constructor for TestOption.
     * @param arg0
     */
    public TestOption(String arg0) {
        super(arg0);
    }
    
    public void testOptionEquals1() {
        Option opt1 = new Option();
        Option opt2 = new Option();
        
        assertTrue("Options should be equal", opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should be equal", opt2.equals(opt1)); //$NON-NLS-1$
    }

    public void testOptionEquals2() {
        Option opt1 = new Option();
        opt1.addDependentGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = new Option();
        opt2.addDependentGroup("abc"); //$NON-NLS-1$
        
        assertTrue("Options should be equal", opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should be equal", opt2.equals(opt1)); //$NON-NLS-1$
    }

    public void testOptionEquals4() {
        Option opt1 = new Option();
        opt1.addNotDependentGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = new Option();
        opt2.addNotDependentGroup("abc"); //$NON-NLS-1$
        
        assertTrue("Options should be equal", opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should be equal", opt2.equals(opt1)); //$NON-NLS-1$
    }

    public void testOptionEquals5() {
        Option opt1 = new Option();
        opt1.addDependentGroup("abc"); //$NON-NLS-1$
        opt1.addNotDependentGroup("xyz"); //$NON-NLS-1$
        Option opt2 = new Option();
        opt2.addDependentGroup("abc"); //$NON-NLS-1$
        opt2.addNotDependentGroup("xyz"); //$NON-NLS-1$
        
        assertTrue("Options should be equal", opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should be equal", opt2.equals(opt1)); //$NON-NLS-1$
    }

    public void testOptionEqualsFail2() {
        Option opt1 = new Option();
        opt1.addDependentGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = new Option();
        
        assertTrue("Options should not be equal", ! opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should not be equal", ! opt2.equals(opt1)); //$NON-NLS-1$
    }
    
    public void testObjectEquivalence1() {
        Option opt1 = new Option();
        opt1.addDependentGroup("abc"); //$NON-NLS-1$

        UnitTestUtil.helpTestEquivalence(0, opt1, opt1);    
    }
    
    public void testObjectEquivalence2() {
        Option opt1 = new Option();
        opt1.addNotDependentGroup("abc"); //$NON-NLS-1$

        UnitTestUtil.helpTestEquivalence(0, opt1, opt1);    
    }
    
    public void testObjectEquivalence3() {
        Option opt1 = new Option();
        opt1.addDependentGroup("abc"); //$NON-NLS-1$
        opt1.addNotDependentGroup("xyz"); //$NON-NLS-1$

        UnitTestUtil.helpTestEquivalence(0, opt1, opt1);    
    }
    
    public void testOptionEquals3() {
        Option opt1 = new Option();
        opt1.addNoCacheGroup("abc"); //$NON-NLS-1$

        UnitTestUtil.helpTestEquivalence(0, opt1, opt1);    
    }
    
    public void testOptionEqualsFail4() {
        Option opt1 = new Option();
        opt1.addNoCacheGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = new Option();
        
        assertTrue("Options should not be equal", ! opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should not be equal", ! opt2.equals(opt1)); //$NON-NLS-1$
    }
    
    public void testOptionEqualsFail5() {
        Option opt1 = new Option();
        opt1.addNoCacheGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = new Option();
        opt1.addNoCacheGroup("abc.def"); //$NON-NLS-1$
        
        assertTrue("Options should not be equal", ! opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should not be equal", ! opt2.equals(opt1)); //$NON-NLS-1$
    }
    
    public void testOptionEqualsFail6() {
        Option opt1 = new Option();
        opt1.addNotDependentGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = new Option();
        
        assertTrue("Options should not be equal", ! opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should not be equal", ! opt2.equals(opt1)); //$NON-NLS-1$
    }
    
    public void testOptionEqualsFail7() {
        Option opt1 = new Option();
        opt1.addDependentGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = new Option();
        opt2.addNotDependentGroup("abc"); //$NON-NLS-1$
        
        assertTrue("Options should not be equal", ! opt1.equals(opt2)); //$NON-NLS-1$
        assertTrue("Options should not be equal", ! opt2.equals(opt1)); //$NON-NLS-1$
    }

    public void testClone() {
        Option opt1 = new Option();
        opt1.addDependentGroup("abc"); //$NON-NLS-1$
        opt1.addNotDependentGroup("xyz"); //$NON-NLS-1$
        opt1.addNoCacheGroup("abc"); //$NON-NLS-1$
        
        Option opt2 = (Option) opt1.clone();
        UnitTestUtil.helpTestEquivalence(0, opt1, opt2);    
    }

    //option NOCACHE with no virtual groups - clone
    public void testDefect15870() {
        Option opt1 = new Option();
        opt1.setNoCache(true); 
        
        Option opt2 = (Option) opt1.clone();
        UnitTestUtil.helpTestEquivalence(0, opt1, opt2);    
    }
    
    public void testGetNotDependentGroups() {
        Option o = new Option();
        o.addDependentGroup("a"); //$NON-NLS-1$
        o.addNotDependentGroup("b"); //$NON-NLS-1$
        o.addNotDependentGroup("c"); //$NON-NLS-1$
        
        assertEquals(Arrays.asList(new Object[] {"b", "c"}), o.getNotDependentGroups()); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
