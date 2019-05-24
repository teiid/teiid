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

package org.teiid.dqp.internal.datamgr;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.language.OrderBy;
import org.teiid.language.SortSpecification;
import org.teiid.query.sql.symbol.ElementSymbol;

public class TestOrderByImpl {

    public static org.teiid.query.sql.lang.OrderBy helpExample() {
        ArrayList<ElementSymbol> elements = new ArrayList<ElementSymbol>();
        elements.add(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        elements.add(TestElementImpl.helpExample("vm1.g1", "e2")); //$NON-NLS-1$ //$NON-NLS-2$
        elements.add(TestElementImpl.helpExample("vm1.g1", "e3")); //$NON-NLS-1$ //$NON-NLS-2$
        elements.add(TestElementImpl.helpExample("vm1.g1", "e4")); //$NON-NLS-1$ //$NON-NLS-2$

        ArrayList<Boolean> types = new ArrayList<Boolean>();
        types.add(Boolean.TRUE);
        types.add(Boolean.FALSE);
        types.add(Boolean.TRUE);
        types.add(Boolean.FALSE);
        return new org.teiid.query.sql.lang.OrderBy(elements, types);
    }

    public static OrderBy example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(), false);
    }

    @Test public void testGetItems() throws Exception {
        List<SortSpecification> items = example().getSortSpecifications();
        assertNotNull(items);
        assertEquals(4, items.size());
    }

}
