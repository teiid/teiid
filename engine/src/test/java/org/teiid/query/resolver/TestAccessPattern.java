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

package org.teiid.query.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.symbol.ElementSymbol;


import junit.framework.TestCase;

public class TestAccessPattern extends TestCase {

    public void testOrdering() {
        
        AccessPattern ap1 = new AccessPattern(createElements(1));
        AccessPattern ap2 = new AccessPattern(createElements(2)); 
        
        List accessPatterns = new ArrayList();
        
        accessPatterns.add(ap2);
        accessPatterns.add(ap1);
        
        Collections.sort(accessPatterns);
        
        assertEquals(ap1, accessPatterns.get(0));
    }
    
    public void testClone() {

        AccessPattern ap2 = new AccessPattern(createElements(2));
        
        AccessPattern clone = (AccessPattern)ap2.clone();
        
        assertNotSame(ap2, clone);
        
        assertEquals(ap2.getUnsatisfied(), clone.getUnsatisfied());
    }

    /** 
     * @return
     */
    private List createElements(int number) {
        List elements = new ArrayList();
        
        for (int i = 0; i < number; i++) {
            elements.add(new ElementSymbol(String.valueOf(i)));
        }
        
        return elements;
    }
    
}
