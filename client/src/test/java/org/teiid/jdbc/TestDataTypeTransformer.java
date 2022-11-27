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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.sql.Clob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialClob;

import org.junit.Test;
import org.teiid.core.types.ArrayImpl;

@SuppressWarnings("nls")
public class TestDataTypeTransformer {

    @Test public void testClobToStringConversion() throws Exception {
        Clob clob = new SerialClob("foo".toCharArray()); //$NON-NLS-1$
        String value = DataTypeTransformer.getString(clob);
        assertEquals("foo", value); //$NON-NLS-1$
    }

    @Test public void testInvalidTransformation() throws Exception {
        try {
            DataTypeTransformer.getDate(Integer.valueOf(1));
            fail("exception expected"); //$NON-NLS-1$
        } catch (SQLException e) {
            assertEquals("Unable to transform the column value 1 to a Date.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testGetDefaultShort() throws Exception {
        assertEquals(0, DataTypeTransformer.getShort(null));
    }

    @Test public void testGetDefaultByte() throws Exception {
        assertEquals(0, DataTypeTransformer.getByte(null));
    }

    @Test public void testGetString() throws Exception {
        assertEquals("", DataTypeTransformer.getString(new SerialClob(new char[0])));
    }

    @Test public void testGetArray() throws Exception {
        assertEquals(new ArrayImpl(new Object[] {1, 2}), DataTypeTransformer.getArray(new int[] {1,2}));
    }

    @Test public void testGetArray1() throws Exception {
        assertEquals(new ArrayImpl(new Object[] {1, 2}), DataTypeTransformer.getArray(new Integer[] {1,2}));
    }

}
