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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;


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

    private DependentSetCriteria example(Object[] vals) {
        ElementSymbol e1 = new ElementSymbol("pm1.g1.e1"); //$NON-NLS-1$
        DependentSetCriteria dsc = new DependentSetCriteria(e1);
        
        final ElementSymbol e2 = new ElementSymbol("pm2.g1.e2"); //$NON-NLS-1$
        dsc.setValueExpression(e2);
        
        FakeValueIteratorSource vis = new FakeValueIteratorSource();        
        vis.addData(e2, vals);
        vis.ready();
        dsc.setValueIteratorSource(vis);
        
        return dsc;
    }
    
    public void testGetValueIterator() throws Exception {
        Object[] vals = new Object[] { "a", "b", "c"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        DependentSetCriteria dsc = example(vals);
        ValueIterator iter = dsc.getValueIterator();
        for(int i=0; iter.hasNext(); i++) {
            assertEquals(vals[i], iter.next());
        }
        assertEquals(false, iter.hasNext());
    }
    
    public void testEquivalence() {
        Object[] vals = new Object[] { "a", "b", "c"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        DependentSetCriteria dsc = example(vals);
        
        UnitTestUtil.helpTestEquivalence(0, dsc, dsc);            
        UnitTestUtil.helpTestEquivalence(0, dsc, dsc.clone());            
    }
    
    public void testEquivalence1() {
        Object[] vals = new Object[] { "a", "b", "c"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        DependentSetCriteria dsc = example(vals);
        DependentSetCriteria dsc1 = example(vals);
        
        dsc1.setValueExpression(new ElementSymbol("foo")); //$NON-NLS-1$
        
        assertNotSame(dsc, dsc1);
    }
    
    static class FakeValueIteratorSource implements ValueIteratorSource {

        private Map data = new HashMap();
        private boolean ready = false;
        
        
        public void addData(Expression valueExpression, Object[] values) {
            data.put(valueExpression, values);            
        }
        
        public void ready() {
            this.ready = true;
        }
        
        public ValueIterator getValueIterator(Expression valueExpression) {
            Object[] vals = (Object[])data.get(valueExpression);
            return new CollectionValueIterator(Arrays.asList(vals));
        }
        
        public boolean isReady() {
            return ready;
        }

    }

}
