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

package org.teiid.vdb.runtime;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestVDBKey {

    @Test public void testCaseInsensitive() {
        VDBKey key = new VDBKey("foo", 1);  //$NON-NLS-1$
        VDBKey key1 = new VDBKey("FOO", 1); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, key, key1);
    }

    @Test public void testNotEqual() {
        VDBKey key = new VDBKey("a", 1);  //$NON-NLS-1$
        VDBKey key1 = new VDBKey("b", 1); //$NON-NLS-1$
        assertFalse(key.equals(key1));
    }

    @Test public void testNameEndingInNumber() {
        VDBKey key = new VDBKey("a1", 1);  //$NON-NLS-1$
        VDBKey key1 = new VDBKey("a", 11); //$NON-NLS-1$
        assertFalse(key.equals(key1));
    }

    @Test public void testDiffertVersion() {
        VDBKey key = new VDBKey("a", 1);  //$NON-NLS-1$
        VDBKey key1 = new VDBKey("a", 11); //$NON-NLS-1$
        assertFalse(key.equals(key1));
    }

    @Test public void testSemanticVersion() {
        VDBKey key = new VDBKey("a.1.2.3", null);  //$NON-NLS-1$
        VDBKey key1 = new VDBKey("a.1.11.3", null); //$NON-NLS-1$
        assertFalse(key.isAtMost());
        assertFalse(key.equals(key1));
        assertEquals(-1, key.compareTo(key1));
        assertEquals(1, key1.compareTo(key));
    }

    @Test public void testShortSemanticVersion() {
        VDBKey key = new VDBKey("a.1.3", null);  //$NON-NLS-1$
        VDBKey key1 = new VDBKey("a", "1.2."); //$NON-NLS-1$
        assertFalse(key.isAtMost());
        assertTrue(key1.isAtMost());
        assertFalse(key.equals(key1));
        assertEquals(1, key.compareTo(key1));
        assertEquals(-1, key1.compareTo(key));
    }

    @Test(expected=TeiidRuntimeException.class) public void testInvalid() {
        new VDBKey("a", "abc");  //$NON-NLS-1$
    }

    @Test public void testPossiblyInvalid() {
        VDBKey key = new VDBKey("a.abc", null);  //$NON-NLS-1$
        assertEquals("a.abc", key.getName());
        assertTrue(key.isAtMost());
        assertEquals("a.abc.latest", key.toString());
    }

    @Test(expected=TeiidRuntimeException.class) public void testInvalidName() {
        new VDBKey("a.1", "1");  //$NON-NLS-1$
    }

    @Test public void testUnicode() {
        VDBKey key = new VDBKey("你好.1", null);
        assertEquals("你好", key.getName());
    }

}
