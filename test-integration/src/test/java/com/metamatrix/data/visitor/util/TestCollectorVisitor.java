/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.data.visitor.util;

import java.util.*;

import com.metamatrix.connector.language.*;
import com.metamatrix.connector.visitor.util.CollectorVisitor;
import com.metamatrix.dqp.internal.datamgr.language.*;

import junit.framework.TestCase;

/**
 */
public class TestCollectorVisitor extends TestCase {

    /**
     * Constructor for TestElementCollectorVisitor.
     * @param name
     */
    public TestCollectorVisitor(String name) {
        super(name);
    }

    public Set getStringSet(Collection objs) {
        Set strings = new HashSet();
        
        Iterator iter = objs.iterator();
        while(iter.hasNext()) {
            Object obj = iter.next();
            if(obj == null) {
                strings.add(null);
            } else {
                strings.add(obj.toString());
            }
        }
        
        return strings;
    }
    
    public void helpTestCollection(ILanguageObject obj, Class type, String[] objects) {
        Set actualObjects = getStringSet(CollectorVisitor.collectObjects(type, obj));
        Set expectedObjects = new HashSet(Arrays.asList(objects));
        
        assertEquals("Did not get expected objects", expectedObjects, actualObjects); //$NON-NLS-1$
    }
 
    public ILanguageObject example1() {
        GroupImpl g = new GroupImpl("g1", null, null); //$NON-NLS-1$
        List symbols = new ArrayList();        
        symbols.add(new ElementImpl(g, "e1", null, String.class)); //$NON-NLS-1$
        IFunction function = new FunctionImpl("length", new IExpression[] { new ElementImpl(g, "e2", null, String.class) }, Integer.class); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(function);
        SelectImpl s = new SelectImpl(symbols, false);
        List groups = new ArrayList();
        groups.add(g);
        FromImpl f = new FromImpl(groups);
        QueryImpl q = new QueryImpl(s, f, null, null, null, null);
             
        return q;   
    }
 
    public void testCollection1() {
        helpTestCollection(example1(), IElement.class, new String[] {"g1.e1", "g1.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCollection2() {
        helpTestCollection(example1(), IFunction.class, new String[] {"length(g1.e2)" }); //$NON-NLS-1$
    }

    public void testCollection3() {
        helpTestCollection(example1(), IExpression.class, new String[] {"g1.e1", "g1.e2", "length(g1.e2)" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }


    public void helpTestElementsUsedByGroups(ILanguageObject obj, String[] elements, String[] groups) {
        Set actualElements = getStringSet(CollectorVisitor.collectElements(obj));
        Set actualGroups = getStringSet(CollectorVisitor.collectGroupsUsedByElements(obj));
        
        Set expectedElements = new HashSet(Arrays.asList(elements));
        Set expectedGroups = new HashSet(Arrays.asList(groups));
        
        assertEquals("Did not get expected elements", expectedElements, actualElements); //$NON-NLS-1$
        assertEquals("Did not get expected groups", expectedGroups, actualGroups);         //$NON-NLS-1$
    }
    
    public void test1() {
        GroupImpl g1 = new GroupImpl("g1", null, null); //$NON-NLS-1$
        ElementImpl e1 = new ElementImpl(g1, "e1", null, String.class); //$NON-NLS-1$
        
        helpTestElementsUsedByGroups(e1, new String[] {"g1.e1"}, new String[] {"g1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void test2() {
        GroupImpl g1 = new GroupImpl("g1", null, null); //$NON-NLS-1$
        ElementImpl e1 = new ElementImpl(g1, "e1", null, String.class); //$NON-NLS-1$
        ElementImpl e2 = new ElementImpl(g1, "e2", null, String.class); //$NON-NLS-1$
        CompareCriteriaImpl cc = new CompareCriteriaImpl(e1, e2, ICompareCriteria.EQ);
        
        helpTestElementsUsedByGroups(cc, new String[] {"g1.e1", "g1.e2"}, new String[] {"g1"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

}
