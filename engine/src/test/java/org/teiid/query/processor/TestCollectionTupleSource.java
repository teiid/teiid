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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;

/**
 */
public class TestCollectionTupleSource {

    @Test public void testNullSource() {
        List<Expression> elements = new ArrayList<Expression>();
        elements.add(new ElementSymbol("x")); //$NON-NLS-1$
        elements.add(new ElementSymbol("y")); //$NON-NLS-1$
        CollectionTupleSource nts = CollectionTupleSource.createNullTupleSource();

        // Walk it and get no data
        List tuple = nts.nextTuple();
        nts.closeSource();

        assertEquals("Didn't get termination tuple for first tuple", null, tuple);             //$NON-NLS-1$
    }

    @Test public void testUpdateCountSource() {
        CollectionTupleSource nts = CollectionTupleSource.createUpdateCountTupleSource(5);

        // Walk it and get no data
        List tuple = nts.nextTuple();
        nts.closeSource();

        assertEquals("Didn't get termination tuple for first tuple", Arrays.asList(5), tuple);             //$NON-NLS-1$
    }


}
