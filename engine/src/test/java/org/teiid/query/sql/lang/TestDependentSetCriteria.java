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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;



/** 
 */
public class TestDependentSetCriteria extends TestCase {

    /** 
     * 
     */
    public TestDependentSetCriteria() {
        super();
    }

    /** 
     * @param name
     */
    public TestDependentSetCriteria(String name) {
        super(name);
    }

    private DependentSetCriteria example() {
        ElementSymbol e1 = new ElementSymbol("pm1.g1.e1"); //$NON-NLS-1$
        DependentSetCriteria dsc = new DependentSetCriteria(e1, ""); //$NON-NLS-1$
        
        final ElementSymbol e2 = new ElementSymbol("pm2.g1.e2"); //$NON-NLS-1$
        dsc.setValueExpression(e2);
        
        return dsc;
    }
    
    public void testEquivalence() {
        DependentSetCriteria dsc = example();
        
        UnitTestUtil.helpTestEquivalence(0, dsc, dsc);            
        UnitTestUtil.helpTestEquivalence(0, dsc, dsc.clone());            
    }
    
    public void testEquivalence1() {
        DependentSetCriteria dsc = example();
        DependentSetCriteria dsc1 = example();
        
        dsc1.setValueExpression(new ElementSymbol("foo")); //$NON-NLS-1$
        
        assertNotSame(dsc, dsc1);
    }
    
}
