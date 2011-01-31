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

package org.teiid.query.function.metadata;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.FunctionParameter;


public class TestFunctionMethod extends TestCase {

    public TestFunctionMethod(String name) {
        super(name);
    }

    public void testEquivalence1() {
        FunctionParameter p1 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        
        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length",  //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p1 }, pout );    
            
        UnitTestUtil.helpTestEquivalence(0, m1, m1);
    }
    
    public void testEquivalence11() {
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        
        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length",  //$NON-NLS-1$ //$NON-NLS-2$
            null, pout );    
            
        UnitTestUtil.helpTestEquivalence(0, m1, m1);
    }    
    
    public void testEquivalence2() {
        FunctionParameter p1 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p1 }, pout );

        FunctionParameter p2 = new FunctionParameter("in", "integer"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout2 = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m2 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p2 }, pout2 );

        UnitTestUtil.helpTestEquivalence(1, m1, m2);
    }

    public void testEquivalence3() {
        FunctionParameter p1 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout = new FunctionParameter("out", "string"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m1 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p1 }, pout );

        FunctionParameter p2 = new FunctionParameter("in", "string"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionParameter pout2 = new FunctionParameter("out", "integer"); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionMethod m2 = new FunctionMethod("length", "", FunctionCategoryConstants.STRING, //$NON-NLS-1$ //$NON-NLS-2$
            "com.metamatrix.query.function.FunctionMethods", "length", //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] { p2 }, pout2 );

        UnitTestUtil.helpTestEquivalence(0, m1, m2);
    }
    
}
