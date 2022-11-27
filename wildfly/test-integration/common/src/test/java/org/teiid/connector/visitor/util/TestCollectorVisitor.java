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

package org.teiid.connector.visitor.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.visitor.CollectorVisitor;
/**
 */
public class TestCollectorVisitor {

    public Set<String> getStringSet(Collection<? extends Object> objs) {
        Set<String> strings = new HashSet<String>();

        for (Object obj : objs) {
            if(obj == null) {
                strings.add(null);
            } else {
                strings.add(obj.toString());
            }
        }

        return strings;
    }

    public void helpTestCollection(LanguageObject obj, Class type, String[] objects) {
        Set actualObjects = getStringSet(CollectorVisitor.collectObjects(type, obj));
        Set expectedObjects = new HashSet(Arrays.asList(objects));

        assertEquals("Did not get expected objects", expectedObjects, actualObjects); //$NON-NLS-1$
    }

    public LanguageObject example1() {
        NamedTable g = new NamedTable("g1", null, null); //$NON-NLS-1$
        List symbols = new ArrayList();
        symbols.add(new ColumnReference(g, "e1", null, String.class)); //$NON-NLS-1$
        Function function = new Function("length", Arrays.asList(new ColumnReference(g, "e2", null, String.class)), Integer.class); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(function);
        List groups = new ArrayList();
        groups.add(g);
        Select q = new Select(symbols, false, groups, null, null, null, null);

        return q;
    }

    @Test public void testCollection1() {
        helpTestCollection(example1(), ColumnReference.class, new String[] {"g1.e1", "g1.e2" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCollection2() {
        helpTestCollection(example1(), Function.class, new String[] {"length(g1.e2)" }); //$NON-NLS-1$
    }

    @Test public void testCollection3() {
        helpTestCollection(example1(), Expression.class, new String[] {"g1.e1", "g1.e2", "length(g1.e2)" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }


    public void helpTestElementsUsedByGroups(LanguageObject obj, String[] elements, String[] groups) {
        Set<String> actualElements = getStringSet(CollectorVisitor.collectElements(obj));
        Set<String> actualGroups = getStringSet(CollectorVisitor.collectGroupsUsedByElements(obj));

        Set<String> expectedElements = new HashSet<String>(Arrays.asList(elements));
        Set<String> expectedGroups = new HashSet<String>(Arrays.asList(groups));

        assertEquals("Did not get expected elements", expectedElements, actualElements); //$NON-NLS-1$
        assertEquals("Did not get expected groups", expectedGroups, actualGroups);         //$NON-NLS-1$
    }

    @Test public void test1() {
        NamedTable g1 = new NamedTable("g1", null, null); //$NON-NLS-1$
        ColumnReference e1 = new ColumnReference(g1, "e1", null, String.class); //$NON-NLS-1$

        helpTestElementsUsedByGroups(e1, new String[] {"g1.e1"}, new String[] {"g1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void test2() {
        NamedTable g1 = new NamedTable("g1", null, null); //$NON-NLS-1$
        ColumnReference e1 = new ColumnReference(g1, "e1", null, String.class); //$NON-NLS-1$
        ColumnReference e2 = new ColumnReference(g1, "e2", null, String.class); //$NON-NLS-1$
        Comparison cc = new Comparison(e1, e2, Operator.EQ);

        helpTestElementsUsedByGroups(cc, new String[] {"g1.e1", "g1.e2"}, new String[] {"g1"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

}
