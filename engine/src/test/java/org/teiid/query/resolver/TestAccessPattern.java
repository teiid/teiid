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
