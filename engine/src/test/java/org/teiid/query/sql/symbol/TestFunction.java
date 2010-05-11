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

package org.teiid.query.sql.symbol;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;

import junit.framework.TestCase;


public class TestFunction extends TestCase {

    // ################################## FRAMEWORK ################################
    
    public TestFunction(String name) { 
        super(name);
    }    
    
    // ################################## TEST HELPERS ################################

    // ################################## ACTUAL TESTS ################################

    public void testFunction1() { 
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(0, f1, f2);        
    }    

    public void testFunction2() { 
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("F1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(0, f1, f2);        
    }    

    public void testFunction3() { 
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("f2", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(1, f1, f2);        
    } 
    
    public void testFunction4() { 
        Function f1 = new Function("f1", new Expression[] {null}); //$NON-NLS-1$
        Function f2 = new Function("f1", new Expression[] {null}); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, f1, f2);        
    }     

    public void testFunction5() { 
        Function f1 = new Function("f1", new Expression[] {null}); //$NON-NLS-1$
        Function f2 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(1, f1, f2);        
    }     

    public void testFunction6() {
        Function f1 = new Function("f1", new Expression[] {new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$
        Function f2 = new Function("f1", new Expression[] {new Constant("xyz"), new Constant("xyz")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        UnitTestUtil.helpTestEquivalence(1, f1, f2);
    }
 
}
