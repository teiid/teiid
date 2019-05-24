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

package org.teiid.metadata;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestTable {

    @Test public void testCardinality() {
        Table t = new Table();
        assertEquals(-1, t.getCardinalityAsFloat(), 0);
        t.setCardinality(1000);
        assertEquals(1000, t.getCardinalityAsFloat(), 0);
        t.setCardinality(100000111000111100L);
        assertEquals(100000111000111100L/t.getCardinalityAsFloat(), 1, .01);
    }

    @Test public void testColumnPrecisionScale() {
        Column c = new Column();
        Datatype datatype = new Datatype();
        datatype.setName("bigdecimal");
        c.setDatatype(datatype);
        c.setPrecision(0);
        c.setScale(2);
        assertEquals(2, c.getScale());
        assertEquals(BaseColumn.DEFAULT_PRECISION, c.getPrecision());
    }

}
