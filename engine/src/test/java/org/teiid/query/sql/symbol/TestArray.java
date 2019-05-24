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

package org.teiid.query.sql.symbol;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.client.BatchSerializer;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.visitor.SQLStringVisitor;

@SuppressWarnings("nls")
public class TestArray {

    @Test public void testArrayValueCompare() {
        ArrayImpl a1 = new ArrayImpl(new Object[] {1, 2, 3});

        UnitTestUtil.helpTestEquivalence(0, a1, a1);

        ArrayImpl a2 = new ArrayImpl(new Object[] {1, 2});

        UnitTestUtil.helpTestEquivalence(1, a1, a2);
    }

    @Test public void testArrayValueEqualsDifferentTypes() {
        ArrayImpl a1 = new ArrayImpl(new Object[] {1, 2, 3});

        ArrayImpl a2 = new ArrayImpl(new Object[] {"1", 2});

        assertFalse(a1.equals(a2));
    }

    @Test public void testArrayValueToString() {
        ArrayImpl a1 = new ArrayImpl(new Object[] {1, "x'2", 3});

        assertEquals("(1, 'x''2', 3)", SQLStringVisitor.getSQLString(new Constant(a1)));

        a1 = new ArrayImpl((Object[])new Object[][] {{1, "x'2"}, {"c"}});

        assertEquals("(('1', 'x''2'), ('c',))", SQLStringVisitor.getSQLString(new Constant(a1)));
    }

    @Test public void testArrayClone() {
        Array array = new Array(DataTypeManager.DefaultDataClasses.OBJECT, Arrays.asList((Expression)new ElementSymbol("e1")));

        Array a1 = array.clone();

        assertNotSame(a1, array);
        assertEquals(a1, array);
        assertNotSame(a1.getExpressions().get(0), array.getExpressions().get(0));
    }

    @SuppressWarnings("unchecked")
    @Test public void testArrayValueSerialization() throws Exception {
        ArrayImpl a1 = new ArrayImpl((Object[])new Integer[] {1, null, 3});
        ArrayImpl a2 = new ArrayImpl((Object[])null);
        String[] types = TupleBuffer.getTypeNames(Arrays.asList(new Array(Integer.class, null)));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        BatchSerializer.writeBatch(oos, types, Arrays.asList(Arrays.asList(a1), Arrays.asList(a2)));
        oos.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        List<List<Object>> batch = BatchSerializer.readBatch(ois, types);
        assertEquals(a1, batch.get(0).get(0));
        try {
            ((java.sql.Array)batch.get(1).get(0)).getArray();
            fail();
        } catch (SQLException e) {

        }
    }

    @Test public void testZeroBasedArray() throws Exception {
        ArrayImpl a1 = new ArrayImpl((Object[])new Integer[] {1, 2, 3});
        a1.setZeroBased(true);
        assertEquals(2, java.lang.reflect.Array.get(a1.getArray(1, 1), 0));
    }

    /**
     * This is for compatibility with array_get
     * @throws Exception
     */
    @Test(expected=IndexOutOfBoundsException.class) public void testIndexOutOfBounds() throws Exception {
        ArrayImpl a1 = new ArrayImpl((Object[])new Integer[] {1, 2, 3});
        a1.getArray(-1, 1);
    }

    @Test public void testSerialize() throws Exception {
        ArrayImpl a1 = new ArrayImpl((Object[])new Integer[] {1, 2, 3});
        a1 = UnitTestUtil.helpSerialize(a1);
        assertEquals(1, a1.getValues()[0]);
    }
}
