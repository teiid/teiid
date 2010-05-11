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
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.*;


/**
 * @author amiller
 *
 * To change this generated comment go to 
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class TestCompoundCriteria extends TestCase {

    /**
     * Constructor for TestCompoundCriteria.
     * @param name
     */
    public TestCompoundCriteria(String name) {
        super(name);
    }

    public void testClone1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        CompareCriteria ccrit1 = new CompareCriteria(e1, CompareCriteria.EQ, new Constant("abc")); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        CompareCriteria ccrit2 = new CompareCriteria(e2, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.AND, ccrit1, ccrit2);
        
        UnitTestUtil.helpTestEquivalence(0, comp, comp.clone());        
    }
    
    public void testClone2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        CompareCriteria ccrit1 = new CompareCriteria(e1, CompareCriteria.EQ, new Constant("abc")); //$NON-NLS-1$
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.AND, ccrit1, null);
        
        UnitTestUtil.helpTestEquivalence(0, comp, comp.clone());        
    }
    
}
